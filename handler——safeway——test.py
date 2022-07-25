from tabnanny import check
import boto3
import json
import paramiko
import os

from datetime import datetime, timedelta, date
from helper_functions.secrets import GpSecrets
from aws_lambda_powertools import Logger
from botocore.errorfactory import ClientError
from helper_functions.exception import GpException
from helper_functions.pandas import GpPandas
from helper_functions import reporting

s3_client = boto3.client("s3")
logger = Logger(service="download-files")
secret = GpSecrets(logger)
pandas_helper = GpPandas(logger)
s3 = boto3.resource("s3")

WINDOW_SIZE = 100000000
MAX_PACKET_SIZE = 100000000


def check_param(params, key, type):
    """Checks param type, returns param if successful

    :param params: a dict with params
    :type param: dict
    :param key: key of the params to check
    :type key: string
    :param type: python datatype to assert. Example: str, int, pd.DataFrame
    :type type: Python data class
    :raises ValueError: raised on failed test
    :return: param from input
    :rtype: any
    """
    if key in params and params[key] is not None:
        if not isinstance(params[key], type):
            raise ValueError(f"Parameter [{params[key]}] is not required type [{type}]")
        return params[key]


def process_error(message, passed_error, **kwargs):
    """Used for error processing in infrastructure. On severity == error, will
    raise GpException

    :param message: detailed message for error
    :type message: str
    :param passed_error: error message from caught exception
    :type passed_error: python compatible Exception class
    :param e_logger: logger for logging error messages
    :type e_logger: python compatible Logger class
    :raises GpException: GpException class
    """

    logger = kwargs["logger"]
    logger.error(str(passed_error))
    logger.error(message)

    report_file_downloads(
        correlation_id,
        search_paths,
        logger,
        data_lake_notifications_topic,
        context,
        event,
        reporting.Status.FAIL,
    )

    raise GpException(
        passed_error,
        message,
        **kwargs,
    )


def get_prefix(source_key_name, **args):
    """Used to get the prefix name of given source path

    :param source_key_name: source key path
    :type : str
    :return: prefix
    :rtype: str

    """
    try:
        prefix = source_key_name.rsplit("/", 1)[0] + "/"
        return prefix
    except Exception as e:
        msg = f"error occured in get_prefix function. Please check logs."
        process_error(msg, e, **args)


def get_key_list(source_bucketname, prefix, **args):
    """Used to get the list of files(sorted by LastModified in descending order) in a bucket

    :param source_bucketname: Bucket name
    :type : str
    :param prefix: key path
    :type : str
    :return: list of file names
    :rtype: list

    """
    try:
        response_key_list = []
        response_key_dict = dict()
        print("prefix of get_key_list:",prefix)
        response = s3_client.list_objects_v2(
            Bucket=source_bucketname, Prefix=prefix, Delimiter="/"
        )
        for content in response.get("Contents", []):
            response_key_dict[content["Key"]] = content["LastModified"].timestamp()
        response_key_list = sorted(
            response_key_dict, key=lambda x: response_key_dict[x], reverse=True
        )
        
        return response_key_list
    except Exception as e:
        msg = f"error occured in get_key_list function. Please check logs."
        process_error(msg, e, **args)


def get_latest_key(
    source_bucketname,
    file_path_prefix,
    file_name_suffix,
    convert_source_key_name_to_lower_case,
    **args,
):
    """Used to get the latest file with the given prefix in a bucket

    :param source_bucketname: Bucket name
    :type : str
    :param prefix: key path prefix
    :type : str
    :return: key of the latest file
    :rtype: str

    """
    if convert_source_key_name_to_lower_case:
        try:
            prefix = os.path.dirname(file_path_prefix) + "/"
            filename_prefix = os.path.basename(file_path_prefix).lower()
            response_key_dict = dict()

            print("Prefix (path): " + prefix)

            response = s3_client.list_objects_v2(
                Bucket=source_bucketname, Prefix=prefix, Delimiter="/"
            )

            for content in response.get("Contents", []):
                filenameOnly = os.path.basename(content["Key"]).lower()
                print("filenameOnly: " + filenameOnly)
                # print("Prefix (path): " + prefix)
                # print("filename_prefix: " + filename_prefix)
                print("file_name_suffix: " + file_name_suffix)

                if filenameOnly.lower().startswith(
                    filename_prefix
                ):
                # and filenameOnly.lower().endswith(file_name_suffix):
                    print("found: " + content["Key"])
                    response_key_dict[content["Key"]] = content[
                        "LastModified"
                    ].timestamp()
            if len(response_key_dict) > 0:
                return response_key_dict
            return prefix
        except Exception as e:
            msg = f"error occured in get_latest_key function. Please check logs."
            process_error(msg, e, **args)
    else:
        try:
            response_key_dict = dict()
            response = s3_client.list_objects_v2(
                Bucket=source_bucketname, Prefix=file_path_prefix, Delimiter="/"
            )
            for content in response.get("Contents", []):
                response_key_dict[content["Key"]] = content["LastModified"].timestamp()
            if len(response_key_dict) > 0:
                return max(response_key_dict, key=response_key_dict.get)
            return prefix
        except Exception as e:
            msg = f"error occured in get_latest_key function. Please check logs."
            process_error(msg, e, **args)


def get_source_key_and_path(source_key, response_key_list, prefix, target_key, **args):
    """Used to get the combination of source key which needs to copy and path to return as list

    :param source_key: Bucket name
    :type : str
    :param response_key_list: key path
    :type : list
    :param prefix: key path
    :type : str
    :param target_key: key path
    :type : str
    :return: source key and full path
    :rtype: tuple

    """
    try:
        matches = []
        final_path = ""
        input_key = source_key.split("/")[-1].split(".")[0]
        input_split_key = input_key.split("_")[0]
        final_source_key = ""
        print("input_key "+input_key)
        print("input_split_key "+input_split_key)
        for final_listed in response_key_list:
            if input_key in final_listed:
                final_match = final_listed.split("/")[-1]
                matches.append(final_match)
        print("matches:",matches)
        for final_source_value in matches:
            if final_source_value.startswith(input_split_key):
                final_source_key = prefix + final_source_value
                break
            
        if final_source_key != "":
            final_src_split = final_source_key.split("/")[-1]
            print("final_src_split: "+ final_src_split)
            target_key_split = target_key.split("/")
            print("target_key_split: "+ target_key_split)
            last_ele = target_key.split("/").pop(-1)
            print("last_ele: "+ last_ele)
            target_key_split.remove(last_ele)
            target_key_split.append(final_src_split)
            print("target_key_split: "+ target_key_split)
            delimit = "/"
            final_path = delimit.join(target_key_split)
        print("final_source_key",final_source_key)
        print("final_source_key",final_source_key)
        return (final_source_key, final_path)
    except Exception as e:
        msg = f"error occured in get_source_key_and_path function. Please check logs."
        process_error(msg, e, **args)


def file_exists(bucket, prefix):
    """Check if the file exists

    :param bucket: bucket name of the file
    :type : str
    :param prefix: key path of the file
    :type : str
    :return: whether the file exists or not
    :rtype: bool

    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if response and "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"] == prefix:
                return True
    return False


def report_file_downloads(
    correlation_id,
    target_key_list_output,
    logger,
    data_lake_notifications_topic,
    context,
    event,
    status,
):
    try:
        # Real code here
        start_time = datetime.utcnow()
        # Reporting log initialization

        for item in target_key_list_output:
            reporting_args = {
                "service_name": reporting.ServiceName.ACQUIRE,
                "correlation_id": str(correlation_id),
                "job_name": context.function_name,
                "job_run_id": str(context.aws_request_id),
                "sdlc": context.function_name.split("-")[0],
                "ingestion_type": reporting.IngestionType.RAW,
                "data_set_name": "",
                "data_asset_name": "",
                "start_time": start_time,
            }
            try:
                reporting_args["data_set_name"] = (item.get("target_key_name")).split(
                    "/"
                )[0]
                reporting_args["data_asset_name"] = (item.get("target_key_name")).split(
                    "/"
                )[1]
            except Exception as e:
                msg = f"Unable to form reporting_args, {e}. Please check properties."
                logger.error(str(e))
                logger.error(msg)

            reporting_log = reporting.GpLambdaReporting(**reporting_args)
            reporting_args["reporting_logger"] = reporting_log
            reporting_log.send_log_event(status)
    except Exception as e:
        msg = f"Unable to form reporting_args, {e}. Please check properties."
        logger.error(str(e))
        logger.error(msg)


def handler(event, context):

    data_lake_notifications_topic = check_param(
        event, "data_lake_notifications_topic", str
    )
    args = {
        "data_lake_notifications_topic": data_lake_notifications_topic,
        "logger": logger,
    }

    target_key_list_output = []
    search_paths = []
    correlation_id = ""

    try:
        status = "Success"
        status_details = {"Success Message": "Files copied successfully!"}
        correlation_id = str(context.aws_request_id)
        if "request_context" in event:
            logger.set_correlation_id(event["request_context"]["request_id"])
            correlation_id = event["request_context"]["request_id"]

        try:
            search_paths = check_param(event, "search_paths", list)

            report_file_downloads(
                correlation_id,
                search_paths,
                logger,
                data_lake_notifications_topic,
                context,
                event,
                reporting.Status.IN_PROCESS,
            )
        except Exception as e:
            msg = f"search_paths input not found in event. Please check logs."
            process_error(msg, e, **args)

        try:
            start_days_back = check_param(event, "start_days_back", int)
        except Exception as e:
            msg = f"start_days_back input not found in event. Please check logs"
            process_error(msg, e, **args)

        try:
            end_days_back = check_param(event, "end_days_back", int)
        except Exception as e:
            msg = f"end_days_back input not found in event. Please check logs."
            process_error(msg, e, **args)

        try:
            job_type_arg = check_param(event, "job_type_arg", str)
        except Exception as e:
            msg = f"job_type_arg input not found in event. Please check logs."
            process_error(msg, e, **args)

        try:
            env = check_param(event, "env", str)
        except Exception as e:
            msg = f"env input not found in event. Please check logs."
            process_error(msg, e, **args)

        try:
            job_name = check_param(event, "job_name", str)
        except Exception as e:
            msg = f"job_name input not found in event. Please check logs."
            process_error(msg, e, **args)

        try:
            day_pattern = check_param(event, "day_pattern", str)
        except Exception as e:
            msg = f"day_pattern input not found in event. Please check logs."
            process_error(msg, e, **args)

        for itvalue in range(0, len(search_paths)):
            search_path = search_paths[itvalue]
            logger.info(
                "Checking if search path has secret_id:"
                + str(search_path.get("secret_id"))
            )

            if search_path.get("secret_id") == None:
                logger.info("secret_id did not exist")
                try:
                    source_bucketname = check_param(
                        search_path, "source_bucketname", str
                    )
                except Exception as e:
                    msg = f"source_bucketname input not found in event. Please check logs."
                    process_error(msg, e, **args)

                try:
                    target_bucketname = check_param(
                        search_path, "target_bucketname", str
                    )
                except Exception as e:
                    msg = f"target_bucketname input not found in event. Please check logs."
                    process_error(msg, e, **args)

                try:
                    if "job_type" in search_path:
                        job_type = check_param(search_path, "job_type", str)
                    else:
                        job_type = ""
                except Exception as e:
                    msg = f"job_type input not found in event. Please check logs."
                    process_error(msg, e, **args)

                try:
                    skip_duplicate_file_download = check_param(
                        search_path, "skip_duplicate_file_download", bool
                    )
                except Exception as e:
                    msg = f"skip_duplicate_file_download input not found in event. Please check logs."
                    process_error(msg, e, **args)

                trg_bucketname = s3.Bucket(target_bucketname)

                for value in range(end_days_back, start_days_back):
                    now = datetime.now() - timedelta(days=value)
                    year = now.strftime("%Y")
                    month = now.strftime("%m")
                    day = now.strftime("%d")
                   
                   

                    week_z_fill = 1

                    if "week_z_fill" in search_path:
                        week_z_fill = search_path["week_z_fill"]

                    prev_week = str(int(now.strftime("%V")) - 1).zfill(week_z_fill)

                    try:
                        if "job_args" in search_path:
                            job_args = check_param(search_path, "job_args", dict)
                        else:
                            job_args = {}
                    except Exception as e:
                        msg = f"job_args input not found in event. Please check logs."
                        process_error(msg, e, **args)

                    try:
                        source_key_name = check_param(
                            search_path, "source_key_name", str
                        )
                        source_key_name = (
                            source_key_name.replace("{YYYY}", year)
                            .replace("{MM}", month)
                            .replace("{DD}", day)
                            .replace("{week}", prev_week)
                        )

                        convert_source_key_name_to_lower_case = False

                        if "convert_source_key_name_to_lower_case" in search_path:
                            convert_source_key_name_to_lower_case = True

                        if "*" in source_key_name:
                            source_key_prefix = source_key_name.split("*")[0].strip("*")
                            source_key_suffix = source_key_name.split("*")[1].strip("*")
                            source_key_name = get_latest_key(
                                source_bucketname,
                                source_key_prefix,
                                source_key_suffix,
                                convert_source_key_name_to_lower_case,
                                **args,
                            )
                        logger.info("souce_key_name:"+str(source_key_name))

                        prefix = get_prefix(source_key_name, **args)
                        response_key_list = get_key_list(
                            source_bucketname, prefix, **args
                        )
                        logger.info("response_key_list:"+str(response_key_list))
                    except Exception as e:
                        msg = f"source_key_name input not found in event. Please check logs."
                        process_error(msg, e, **args)

                    try:
                        target_key_name = check_param(
                            search_path, "target_key_name", str
                        )
                    except Exception as e:
                        msg = f"target_key_name input not found in event. Please check logs."
                        process_error(msg, e, **args)

                    uncompress = None

                    try:
                        if "uncompress" in search_path:
                            uncompress = check_param(search_path, "uncompress", dict)
                            uncompress_str = json.dumps(uncompress)
                            uncompress_str = (
                                uncompress_str.replace("{YYYY}", year)
                                .replace("{MM}", month)
                                .replace("{DD}", day)
                                .replace("{week}", prev_week)
                                .replace("{correlation_id}", correlation_id)
                            )
                            uncompress_str = pandas_helper.format_directory(
                                uncompress_str
                            )
                            uncompress = json.loads(uncompress_str)

                    except Exception as e:
                        msg = (
                            f"uncompressed input not found in event. Please check logs."
                        )
                        process_error(msg, e, **args)

                    source_key = (
                        source_key_name.replace("{YYYY}", year)
                        .replace("{MM}", month)
                        .replace("{DD}", day)
                        .replace("{week}", prev_week)
                    )
                    logger.info("source_key:"+str(source_key))
                    target_key = (
                        target_key_name.replace("{YYYY}", year)
                        .replace("{MM}", month)
                        .replace("{DD}", day)
                        .replace("{week}", prev_week)
                    )
                    logger.info("target_key:"+str(target_key))
                    source_key_and_path = get_source_key_and_path(
                        source_key, response_key_list, prefix, target_key, **args
                    )
                    final_source_key = source_key_and_path[0]
                    final_path = source_key_and_path[1]
                    logger.info("final_source_key:"+str(final_source_key))
                    logger.info("final_path:"+str(final_path))
                    
                    if "retain_path" not in event:
                        final_path = pandas_helper.format_directory(final_path)
                    logger.info("Final_path: " + final_path)
                    final_path_display = final_path
                    if final_source_key != "":
                        if not skip_duplicate_file_download or not file_exists(
                            target_bucketname, final_path
                        ):
                            target_object = trg_bucketname.Object(final_path)
                            source_object = {
                                "Bucket": source_bucketname,
                                "Key": final_source_key,
                            }
                            try:
                                s3_client.head_object(
                                    Bucket=source_bucketname, Key=final_source_key
                                )
                                logger.info(
                                    "Checking if the file exist:"
                                    + source_bucketname
                                    + "/"
                                    + final_source_key
                                )
                                target_object.copy(source_object)

                                job_type = (
                                    job_type.replace("{YYYY}", year)
                                    .replace("{MM}", month)
                                    .replace("{DD}", day)
                                    .replace("{week}", prev_week)
                                    .replace("{correlation_id}", correlation_id)
                                )
                                job_args_text = json.dumps(job_args)
                                job_args_text = (
                                    job_args_text.replace("{YYYY}", year)
                                    .replace("{MM}", month)
                                    .replace("{DD}", day)
                                    .replace("{week}", prev_week)
                                    .replace("{correlation_id}", correlation_id)
                                )
                                job_args = json.loads(job_args_text)

                                if uncompress is None:

                                    data_asset = final_path_display.split("/")[1]
                                    job_args["source_system_name"] = event.get(
                                        "source_system_name"
                                    )
                                    job_args["source_table_name"] = data_asset
                                    job_args["sdlc"] = context.function_name.split("-")[
                                        0
                                    ]
                                    job_args[
                                        "data_lake_notifications_topic"
                                    ] = event.get("data_lake_notifications_topic")
                                    job_args["enable_metrics"] = event.get(
                                        "enable_metrics", "true"
                                    )
                                    job_args["source_path"] = (
                                        "s3://"
                                        + event.get("stage_bucket")
                                        + "/"
                                        + "/".join(final_path_display.split("/")[:-1])
                                    )
                                    job_args["src_prefix"] = final_path_display
                                    target_key_list_output.append(
                                        {
                                            "file_info": {
                                                "s3_uri": final_path_display,
                                                "YYYY": year,
                                                "MM": month,
                                                "DD": day,
                                                "job_type": job_type,
                                                "job_args": job_args,
                                            }
                                        }
                                    )
                                else:
                                    uncompress_str = json.dumps(uncompress)
                                    uncompress_str = (
                                        uncompress_str.replace(
                                            "{s3_uri}", final_path_display
                                        )
                                        .replace("{YYYYMM}", year + month)
                                        .replace("{yyyymm}", year + month)
                                    )
                                    uncompress_str = pandas_helper.format_directory(
                                        uncompress_str
                                    )
                                    uncompress = json.loads(uncompress_str)
                                    target_key_list_output.append(
                                        {
                                            "file_info": {
                                                "s3_uri": final_path_display,
                                                "YYYY": year,
                                                "MM": month,
                                                "DD": day,
                                                "job_type": job_type,
                                                "job_args": job_args,
                                                "uncompress": uncompress,
                                            }
                                        }
                                    )

                                logger.info(
                                    "File copied from s3 path "
                                    + source_bucketname
                                    + "/"
                                    + final_source_key
                                    + " to "
                                    + target_bucketname
                                    + "/"
                                    + final_path
                                )

                            except ClientError as e:
                                msg = f"ClientError"
                                process_error(msg, e, **args)
                        else:
                            logger.info(
                                "skip_duplicate_file_download is set to true and the file already exists"
                            )

            else:
                try:
                    secret_id = check_param(search_path, "secret_id", str)
                except Exception as e:
                    msg = f"secret_id input not found in event. Please check logs."
                    process_error(msg, e, **args)

                try:
                    target_bucketname = check_param(
                        search_path, "target_bucketname", str
                    )
                except Exception as e:
                    msg = f"target_bucketname input not found in event. Please check logs."
                    process_error(msg, e, **args)

                try:
                    skip_duplicate_file_download = check_param(
                        search_path, "skip_duplicate_file_download", bool
                    )
                except Exception as e:
                    msg = f"skip_duplicate_file_download input not found in event. Please check logs."
                    process_error(msg, e, **args)

                logger.info("secret_id exists copying files from sftp to s3")

                db_credentials = secret.get_secret(
                    secret_id=secret_id,
                    data_lake_notifications_topic=data_lake_notifications_topic,
                )
                db_credentials = json.loads(db_credentials["SecretString"])
                host_name = db_credentials["host_name"]
                username = db_credentials["username"]
                password = db_credentials["password"]
                port = db_credentials["port"]

                transport = paramiko.Transport((host_name, int(port)))
                transport.connect(username=username, password=password)
                sftp = paramiko.SFTPClient.from_transport(
                    transport, WINDOW_SIZE, MAX_PACKET_SIZE
                )

                for value in range(end_days_back, start_days_back):

                    now = datetime.now() - timedelta(days=value)
                    year = now.strftime("%Y")
                    month = now.strftime("%m")
                    day = now.strftime("%d")
                    prev_week = str(int(now.strftime("%V")) - 1)

                    try:
                        server_path = check_param(search_path, "server_path", str)
                    except Exception as e:
                        msg = (
                            f"server_path input not found in event. Please check logs."
                        )
                        process_error(msg, e, **args)

                    try:
                        target_key_name = check_param(
                            search_path, "target_key_name", str
                        )
                    except Exception as e:
                        msg = f"target_key_name input not found in event. Please check logs."
                        process_error(msg, e, **args)

                    last_modified_newer_than = ""
                    try:
                        last_modified_newer_than = check_param(
                            search_path, "last_modified_newer_than", str
                        )
                    except Exception as e:
                        msg = f"last_modified_newer_than input not found in event. Please check logs."
                        process_error(msg, e, **args)

                    uncompress = {}
                    try:
                        uncompress = check_param(search_path, "uncompress", dict)
                        uncompress_str = json.dumps(uncompress)
                        uncompress_str = (
                            uncompress_str.replace("{YYYY}", year)
                            .replace("{MM}", month)
                            .replace("{DD}", day)
                            .replace("{correlation_id}", correlation_id)
                        )
                        uncompress_str = pandas_helper.format_directory(uncompress_str)
                        uncompress = json.loads(uncompress_str)
                    except Exception as e:
                        msg = (
                            f"uncompressed input not found in event. Please check logs."
                        )
                        process_error(msg, e, **args)

                    server_path_file = (
                        server_path.replace("{YYYY}", year)
                        .replace("{MM}", month)
                        .replace("{DD}", day)
                    )
                    file_name = server_path_file.split("/")[-1]
                    local_path = "/tmp/" + file_name
                    last_modified_newer_than_timestamp = ""
                    last_modified_older_than_timestamp = ""
                    if last_modified_newer_than != "":
                        last_modified_newer_than = (
                            last_modified_newer_than.replace("{YYYY}", year)
                            .replace("{MM}", month)
                            .replace("{DD}", day)
                            .replace("{week}", prev_week)
                        )
                        last_modified_newer_than_timestamp = datetime.strptime(
                            last_modified_newer_than, "%Y-%m-%d"
                        )
                        last_modified_older_than_timestamp = datetime.strptime(
                            last_modified_newer_than, "%Y-%m-%d"
                        ) + timedelta(days=1)

                        try:
                            var_files = dict()
                            for i in sftp.listdir():
                                lstatout = str(sftp.lstat(i)).split()[0]
                                if "d" in lstatout:
                                    logger.info(str(i) + " is a directory")
                                else:
                                    logger.info(str(i) + " is a file")
                                    current_file = server_path_file.replace("*", i)
                                    utime = sftp.stat(current_file).st_mtime
                                    last_modified = datetime.fromtimestamp(utime)
                                    logger.info(
                                        "filename: "
                                        + current_file
                                        + "; last_modified: "
                                        + str(last_modified)
                                    )
                                    if (
                                        last_modified_newer_than_timestamp != ""
                                        and last_modified
                                        >= last_modified_newer_than_timestamp
                                        and last_modified
                                        < last_modified_older_than_timestamp
                                    ):
                                        var_files[i] = utime
                                    elif last_modified_newer_than_timestamp == "":
                                        var_files[i] = utime
                            if len(var_files) > 0:
                                latest_file = max(var_files, key=var_files.get)
                                logger.info("latest_file: " + latest_file)

                                server_path_file = server_path_file.replace(
                                    "*", latest_file
                                )
                                local_path = local_path.replace("*", latest_file)
                                utime = sftp.stat(server_path_file).st_mtime
                                last_modified = datetime.fromtimestamp(utime)
                                last_modified_year = last_modified.strftime("%Y")
                                last_modified_month = last_modified.strftime("%m")
                                last_modified_day = last_modified.strftime("%d")

                                sftp.get(server_path_file, local_path)
                                object_name = (
                                    target_key_name.replace(
                                        "{LAST_MODIFIED_YYYY}", last_modified_year
                                    )
                                    .replace("{LAST_MODIFIED_MM}", last_modified_month)
                                    .replace("{LAST_MODIFIED_DD}", last_modified_day)
                                )
                                if "retain_path" not in event:
                                    object_name = pandas_helper.format_directory(
                                        object_name
                                    )
                                logger.info("Object name: " + object_name)
                                if not skip_duplicate_file_download or not file_exists(
                                    target_bucketname, target_key_name
                                ):
                                    s3_client.upload_file(
                                        local_path, target_bucketname, object_name
                                    )
                                    os.remove(local_path)
                                    uncompress_str = json.dumps(uncompress)
                                    uncompress_str = uncompress_str.replace(
                                        "{s3_uri}", object_name
                                    )
                                    uncompress_str = pandas_helper.format_directory(
                                        uncompress_str
                                    )
                                    uncompress = json.loads(uncompress_str)
                                    target_key_list_output.append(
                                        {
                                            "file_info": {
                                                "s3_uri": object_name,
                                                "YYYY": last_modified_year,
                                                "MM": last_modified_month,
                                                "DD": last_modified_day,
                                                "uncompress": uncompress,
                                            }
                                        }
                                    )
                                    logger.info(
                                        "File copied from sftp path "
                                        + server_path_file
                                        + " to "
                                        + target_bucketname
                                        + "/"
                                        + target_key_name
                                    )
                                else:
                                    logger.info(
                                        "skip_duplicate_file_download is set to true and the file already exists"
                                    )
                        except FileNotFoundError:
                            pass
                    else:
                        try:
                            sftp.get(server_path_file, local_path)
                            object_name = (
                                target_key_name.replace("{YYYY}", year)
                                .replace("{MM}", month)
                                .replace("{DD}", day)
                            )
                            if "retain_path" not in event:
                                object_name = pandas_helper.format_directory(
                                    object_name
                                )
                            logger.info("Object name: " + object_name)
                            if not skip_duplicate_file_download or not file_exists(
                                target_bucketname, target_key_name
                            ):
                                s3_client.upload_file(
                                    local_path, target_bucketname, object_name
                                )
                                uncompress_str = json.dumps(uncompress)
                                uncompress_str = uncompress_str.replace(
                                    "{s3_uri}", object_name
                                )
                                uncompress_str = pandas_helper.format_directory(
                                    uncompress_str
                                )
                                uncompress = json.loads(uncompress_str)

                                target_key_list_output.append(
                                    {
                                        "file_info": {
                                            "s3_uri": object_name,
                                            "YYYY": year,
                                            "MM": month,
                                            "DD": day,
                                            "uncompress": uncompress,
                                        }
                                    }
                                )
                                logger.info(
                                    "File copied from sftp path "
                                    + server_path_file
                                    + " to "
                                    + target_bucketname
                                    + "/"
                                    + target_key_name
                                )
                            else:
                                logger.info(
                                    "skip_duplicate_file_download is set to true and the file already exists"
                                )
                        except FileNotFoundError:
                            pass

                transport.close()

                logger.info("Loaded data from sftp to s3")

    except Exception as e:
        status = "Failed"
        exception_kwargs = {
            "logger": logger,
            "job_name": context.function_name,
            "job_id": context.invoked_function_arn,
            "source": "Download File Lambda",
            "asset": "Download File",
            "data_lake_notifications_topic": data_lake_notifications_topic,
        }

        logger.warning("Download file lambda failed due to - " + str(e))
        status_details = {
            "failed_reason": "Download file lambda failed due to - " + str(e)
        }
        raise GpException(
            e,
            str(e),
            **exception_kwargs,
        )

    finally:
        report_file_downloads(
            correlation_id,
            search_paths,
            logger,
            data_lake_notifications_topic,
            context,
            event,
            reporting.Status.PASS,
        )
        return {"job_params": "", "downloaded_file_list": target_key_list_output}
