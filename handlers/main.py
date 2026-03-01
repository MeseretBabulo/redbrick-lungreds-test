import logging
import subprocess
import io
import json
import os
import pandas as pd
from datetime import date, datetime
from google.cloud import secretmanager, storage
import redbrick


def create_a_data_frame(file):
    """This will create a DataFrame for JSON"""
    if not file:
        return pd.DataFrame()

    try:
        df = pd.DataFrame.from_dict(file)
        return df
    except Exception as e:
        logging.error(f"Error creating DataFrame from files: {e}")
        return pd.DataFrame()


def check_rank(rank, datas):
    """Checks duplicates in Ranks"""
    new_rank = []
    for r in rank:
        if r != "----":
            new_rank.append(int(r))
    sets = list(set(new_rank))
    flagged = ""
    if len(new_rank) != len(sets):
        flagged = "Duplicate Ranks,"
    if len(new_rank) == 0:
        flagged = "Missing Attributes,"

    count = 0
    for data in datas:
        if data["Nodule Location"] != "----":
            if data["Nodule Suspicion Rank (1-5)"] == "----" and len(sets) != 5:
                data["Flagged"] += "Missing Rank,"

            if data["Nodule Suspicion Rank (1-5)"] != "----":
                count += 1
                data["Flagged"] += flagged

    for data in datas:
        if count > 0 and len(sets) > 0:
            if count != sets[-1]:
                data["Flagged"] += "Missing Rank,"


def data_values():
    """The values needed to create a row"""
    data = {}
    data["Task ID"] = "----"
    data["Name"] = "----"
    data["Clinician Name"] = "----"
    data["Updated At"] = "----"
    data["Status"] = "----"
    data["Stage"] = "----"
    data["Nodule Centroid"] = "----"
    data["Nodule Location"] = "----"
    data["Nodule Type"] = "----"
    data["Confidence on Nodule Type"] = "----"
    data["Comments on Nodule Type"] = "----"
    data["Nodule Morphology"] = "----"
    data["Confidence on Nodule Morphology"] = "----"
    data["Comments on Nodule Morphology"] = "----"
    data["Nodule-wise LungRADS Score"] = "----"
    data["Confidence on LungRADS Score"] = "----"
    data["Comments on LungRADS Score"] = "----"
    data["Nodule Suspicion Rank (1-5)"] = "----"
    data["Entity Comments"] = "----"
    data["Nodule Volume 2D Mean Diameter"] = "----"
    data["Nodule Volume 2D Max Diameter"] = "----"
    data["Nodule Volume 2D Min Diameter"] = "----"
    data["Nodule Core 2D Mean Diameter (Only for part-solid nodules)"] = "----"
    data["Nodule Core 2D Max Diameter (Only for part-solid nodules)"] = "----"
    data["Nodule Core 2D Min Diameter (Only for part-solid nodules)"] = "----"
    data["Classification (Study Reviewed?)"] = "----"
    data["Classification (Case-wise LungRADS Score)"] = "----"
    data["Classification (Confidence on LungRADS Score)"] = "----"
    data["Classification (Comments on LungRADS Score)"] = "----"
    data["Flagged"] = ""
    return data


def check_data_to_be_flagged(data, groups):
    """Check if Data has something to be flagged"""
    attributes = [
        data["Nodule Location"],
        data["Nodule Type"],
        data["Confidence on Nodule Type"],
        data["Nodule Morphology"],
        data["Confidence on Nodule Morphology"],
        data["Nodule-wise LungRADS Score"],
        data["Confidence on LungRADS Score"],
    ]
    classification = [
        data["Classification (Study Reviewed?)"],
        data["Classification (Case-wise LungRADS Score)"],
        data["Classification (Confidence on LungRADS Score)"],
    ]

    part_solid = [
        data["Nodule Core 2D Mean Diameter (Only for part-solid nodules)"],
        data["Nodule Core 2D Max Diameter (Only for part-solid nodules)"],
        data["Nodule Core 2D Min Diameter (Only for part-solid nodules)"],
    ]

    static = [
        data["Nodule Volume 2D Mean Diameter"],
        data["Nodule Volume 2D Max Diameter"],
        data["Nodule Volume 2D Min Diameter"],
    ]

    if data["Nodule Location"] == "----" and data["Nodule Suspicion Rank (1-5)"] != "----":
        data["Flagged"] += "Unnecessary Rank,"

    if "----" in attributes and data["Nodule Location"] != "----":
        data["Flagged"] += "Missing Attributes,"

    if "----" in classification:
        data["Flagged"] += "Missing Classifications,"

    if data["Nodule Type"] == "Part-solid":
        if "----" in part_solid:
            data["Flagged"] += "Missing Part-solid Data,"

    if data["Nodule Location"] != "----":
        if "----" in static:
            data["Flagged"] += "Missing Measure of Center,"

        if not groups:
            data["Flagged"] += "Volume not Linked,"

    if data["Nodule Suspicion Rank (1-5)"] == "1":
        if data["Classification (Case-wise LungRADS Score)"].split(" ")[0] != data["Nodule-wise LungRADS Score"].split(" ")[0]:
            data["Flagged"] += "LungRADS Score Mismatch,"

    return data


def empty_data(row):
    """This will get the data for no nodules"""
    rows = []
    data = data_values()
    data["Task ID"] = row["taskId"]
    data["Name"] = row["name"]
    if row.get("currentStageName"):
        data["Stage"] = row.get("currentStageName")
    if row.get("status"):
        data["Stage"] = row.get("currentStageName")
    rows.append(data)
    return rows


def empty_consensus(row, task, data):
    """This will get the data for no nodules"""
    rows = []
    data = data_values()
    data["Task ID"] = row["taskId"]
    data["Name"] = row["name"]
    if task.get("updatedBy"):
        data["Clinician Name"] = task.get("updatedBy")
    if task.get("updatedAt"):
        date = datetime.fromisoformat(task.get("updatedAt"))
        formatted_str = date.strftime("%Y:%m:%d %H:%M:%S")
        data["Updated At"] = formatted_str
    if row.get("currentStageName"):
        data["Stage"] = row.get("currentStageName")
    if task.get("status"):
        data["Status"] = task.get("status")
    rows.append(data)
    return rows


def check_data_to_be_flagged_for_no_nodule(data):
    """Check data to be flagged"""
    data["Classification (Study Reviewed?)"]

    if data["Classification (Study Reviewed?)"] == "----":
        data["Flagged"] += "Missing Classifications,"

    return data


def no_nodule(row, task, classification, data, segment_path):
    """Data from No Consensus"""
    rows = []
    data["Task ID"] = row["taskId"]
    data["Name"] = row["name"]
    if task.get("updatedBy"):
        data["Clinician Name"] = task.get("updatedBy")
    if task.get("updatedAt"):
        date = datetime.fromisoformat(task.get("updatedAt"))
        formatted_str = date.strftime("%Y:%m:%d %H:%M:%S")
        data["Updated At"] = formatted_str
    if row.get("currentStageName"):
        data["Stage"] = row.get("currentStageName")
    if task.get("status"):
        data["Status"] = task.get("status")

    if classification:
        attributes = classification.get("attributes")
        if attributes:
            if attributes.get("Study Reviewed?"):
                data["Classification (Study Reviewed?)"] = attributes.get("Study Reviewed?")
            if attributes.get("Case-wise LungRADS Score"):
                data["Classification (Case-wise LungRADS Score)"] = attributes.get("Case-wise LungRADS Score")
            if attributes.get("Confidence on LungRADS Score"):
                data["Classification (Confidence on LungRADS Score)"] = attributes.get("Confidence on LungRADS Score")
            if attributes.get("Comments on LungRADS Score"):
                data["Classification (Comments on LungRADS Score)"] = attributes.get("Comments on LungRADS Score")

    flagged_data = check_data_to_be_flagged_for_no_nodule(data)
    rows.append(flagged_data)
    return rows


def normalize_segment_entries(segment_map):
    """Return segmentMap entries as a list of dicts for both list/dict inputs."""
    if not segment_map:
        return []
    if isinstance(segment_map, dict):
        return [entry for entry in segment_map.values() if isinstance(entry, dict)]
    if isinstance(segment_map, list):
        return [entry for entry in segment_map if isinstance(entry, dict)]
    return []


def check_nodule_segment_path(data, segment_path):
    """Check whether current nodule group exists in segmentMap."""
    if data["Nodule Location"] == "----":
        return data

    nodule_group = data["Nodule Centroid"]
    has_path = False
    for segment in normalize_segment_entries(segment_path):
        if segment.get("group") == nodule_group:
            has_path = True
            break
    if not has_path:
        data["Flagged"] += "Missed path,"
    return data


def get_task_data(row, task, nodule, volume_measures, groups, maps, classification, data, segment_path):
    """Data from Super Task"""
    rows = []
    data["Task ID"] = row["taskId"]
    data["Name"] = row["name"]
    if task.get("updatedBy"):
        data["Clinician Name"] = task.get("updatedBy")
    if task.get("updatedAt"):
        date = datetime.fromisoformat(task.get("updatedAt"))
        formatted_str = date.strftime("%Y:%m:%d %H:%M:%S")
        data["Updated At"] = formatted_str
    if row.get("currentStageName"):
        data["Stage"] = row.get("currentStageName")
    if task.get("status"):
        data["Status"] = task.get("status")
    if nodule.get("group"):
        data["Nodule Centroid"] = nodule.get("group")
    if nodule.get("attributes"):
        attributes = nodule.get("attributes")
        if attributes.get("Nodule Location"):
            data["Nodule Location"] = attributes.get("Nodule Location")
        if attributes.get("Nodule Type"):
            data["Nodule Type"] = attributes.get("Nodule Type")
        if attributes.get("Confidence on Nodule Type"):
            data["Confidence on Nodule Type"] = attributes.get("Confidence on Nodule Type")
        if attributes.get("Comments on Nodule Type"):
            data["Comments on Nodule Type"] = attributes.get("Comments on Nodule Type")
        if attributes.get("Nodule Morphology"):
            data["Nodule Morphology"] = attributes.get("Nodule Morphology")
        if attributes.get("Confidence on Nodule Morphology"):
            data["Confidence on Nodule Morphology"] = attributes.get("Confidence on Nodule Morphology")
        if attributes.get("Comments on Nodule Morphology"):
            data["Comments on Nodule Morphology"] = attributes.get("Comments on Nodule Morphology")
        if attributes.get("Nodule-wise LungRADS Score"):
            data["Nodule-wise LungRADS Score"] = attributes.get("Nodule-wise LungRADS Score")
        if attributes.get("Confidence on LungRADS Score"):
            data["Confidence on LungRADS Score"] = attributes.get("Confidence on LungRADS Score")
        if attributes.get("Comments on LungRADS Score"):
            data["Comments on LungRADS Score"] = attributes.get("Comments on LungRADS Score")
        if attributes.get("Nodule Suspicion Rank (1-5)"):
            data["Nodule Suspicion Rank (1-5)"] = attributes.get("Nodule Suspicion Rank (1-5)")
        if attributes.get("Entity Comments"):
            data["Entity Comments"] = attributes.get("Entity Comments")
        if (volume_measures) and (volume_measures != 0):
            group = nodule.get("group")
            for volume in volume_measures:
                if volume.get("group") and volume["group"] == group:
                    if volume["category"] == "Nodule Volume 2D Min Diameter":
                        data["Nodule Volume 2D Min Diameter"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Volume 2D Max Diameter":
                        data["Nodule Volume 2D Max Diameter"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Volume 2D Mean Diameter":
                        data["Nodule Volume 2D Mean Diameter"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Core 2D Min Diameter (Only for part-solid nodules)":
                        data["Nodule Core 2D Min Diameter (Only for part-solid nodules)"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Core 2D Max Diameter (Only for part-solid nodules)":
                        data["Nodule Core 2D Max Diameter (Only for part-solid nodules)"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Core 2D Mean Diameter (Only for part-solid nodules)":
                        data["Nodule Core 2D Mean Diameter (Only for part-solid nodules)"] = round(volume["length"], 4)

    if classification:
        attributes = classification.get("attributes")
        if attributes:
            if attributes.get("Study Reviewed?"):
                data["Classification (Study Reviewed?)"] = attributes.get("Study Reviewed?")
            if attributes.get("Case-wise LungRADS Score"):
                data["Classification (Case-wise LungRADS Score)"] = attributes.get("Case-wise LungRADS Score")
            if attributes.get("Confidence on LungRADS Score"):
                data["Classification (Confidence on LungRADS Score)"] = attributes.get("Confidence on LungRADS Score")
            if attributes.get("Comments on LungRADS Score"):
                data["Classification (Comments on LungRADS Score)"] = attributes.get("Comments on LungRADS Score")

    flagged_data = check_data_to_be_flagged(data, groups)
    flagged_data = check_nodule_segment_path(flagged_data, segment_path)
    rows.append(flagged_data)
    return rows


def return_a_list_of_groups(maps):
    """Create a sorted list of GroupID"""
    if maps:
        list_of_groups = []
        count = 0
        for value in maps.values():
            group = value.get("group")
            count += 1
            if group:
                list_of_groups.append(group)
        if count != len(list_of_groups):
            return False
        else:
            return "----"
    else:
        return "----"


def check_if_task_has_consensus(row):
    """Check if Task has consensus"""
    super_truth = row.get("superTruth")
    consensus = row.get("consensusTasks")
    rows = []
    data = data_values()

    if super_truth and type(super_truth) != float:
        nodules = super_truth["series"][0].get("landmarks3d")
        volume_measures = super_truth["series"][0].get("measurements")
        maps = super_truth["series"][0].get("segmentMap", None)
        groups = return_a_list_of_groups(maps)
        segment_path = super_truth["series"][0].get("segmentMap", None)
        classification = super_truth.get("classification")

        if nodules and nodules != 0:
            nodule_rows = []
            ranks = []
            datas = []
            for nodule in nodules:
                data = data_values()
                datas = get_task_data(
                    row, super_truth, nodule, volume_measures, groups, maps, classification, data, segment_path
                )
                ranks.append(datas[0]["Nodule Suspicion Rank (1-5)"])
                nodule_rows.extend(datas)
            check_rank(ranks, nodule_rows)
            rows.extend(nodule_rows)

        else:
            data = data_values()
            datas = no_nodule(row, super_truth, classification, data, segment_path)
            rows.extend(datas)

    if consensus and len(consensus) == 3:
        for task in consensus:
            nodules = task["series"][0].get("landmarks3d")
            volume_measures = task["series"][0].get("measurements")
            classification = task.get("classification")
            maps = task["series"][0].get("segmentMap", None)
            groups = return_a_list_of_groups(maps)
            segment_path = task["series"][0].get("segmentMap", None)
            if nodules and nodules != 0:
                nodule_rows = []
                ranks = []  
                datas = []
                for nodule in nodules:
                    data = data_values()
                    datas = get_task_data(
                        row, task, nodule, volume_measures, groups, maps, classification, data, segment_path    
                    )
                    ranks.append(datas[0]["Nodule Suspicion Rank (1-5)"])
                    nodule_rows.extend(datas)
                check_rank(ranks, nodule_rows)
                rows.extend(nodule_rows)

            else:
                data = data_values()
                datas = no_nodule(row, task, classification, data, segment_path)
                rows.extend(datas)
    else:
        datas = empty_data(row)
        rows.extend(datas)
    return pd.DataFrame(rows)


def recreate_new_dataframe(df):
    """Recreate a new dataframe"""
    row = pd.concat(df.apply(check_if_task_has_consensus, axis=1).tolist(), ignore_index=True)
    return row


def iterator_to_json(task_iterator, destination, file):
    """
    Converts an iterator of OutputTask objects to a JSON string.
    """
    output_filepath = os.path.join(destination, file)

    try:
        with open(output_filepath, "w", encoding="utf-8") as f:
            list_of_tasks = []
            for task in task_iterator:
                dict_of_tasks = {
                    "taskId": task.get("taskId"),
                    "name": task.get("name"),
                    "series": task.get("series"),
                    "classification": task.get("classification"),
                    "priority": task.get("priority"),
                    "metaData": task.get("metaData"),
                    "currentStageName": task.get("currentStageName"),
                    "status": task.get("status"),
                    "createdBy": task.get("createdBy"),
                    "createdAt": task.get("createdAt"),
                    "storageId": task.get("storageId"),
                    "updatedBy": task.get("updatedBy"),
                    "updatedByUserId": task.get("updatedByUserId"),
                    "updatedAt": task.get("updatedAt"),
                    "consensus": task.get("consensus"),
                    "consensusScore": task.get("consensusScore"),
                    "consensusTasks": task.get("consensusTasks"),
                    "scores": task.get("scores"),
                    "superTruth": task.get("superTruth"),
                    "datapointClassification": task.get("datapointClassification")
                }
                list_of_tasks.append(dict_of_tasks)

            json.dump(list_of_tasks, f, ensure_ascii=False, indent=4)
        print(f"JSON data successfully saved to {output_filepath}")

    except Exception as e:
        print(f"Error saving JSON data: {e}")


def get_api():
    """Retrieves a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/project022/secrets/api-key/versions/1"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_org():
    """Retrieves a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/project022/secrets/org/versions/1"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_110_project():
    """Retrieves a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/project022/secrets/project_test/versions/1"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


FOLDER_110 = "LungRADS-82-PriorScans-200-single-time-point-"
move_to_110 = f"/app/{FOLDER_110}"
export_file_110 = f"/app/{FOLDER_110}/tasks.json"

destination_110_json_name = f"{date.today()}-redbrick-lungreds-test-json-input.json"
destination_110_csv_name = f"{date.today()}-redbrick-lungreds-test-csv-ouput.csv"


def run_organization():
    """Run RedBrick Organization"""
    logging.warning(f"Running data export script")
    try:
        api_key = get_api()
        org_id = get_org()
        project_110_id = get_110_project()

        os.makedirs(move_to_110, exist_ok=True)
        file = "tasks.json"

        project_110 = redbrick.get_project(api_key=api_key, org_id=org_id, project_id=project_110_id)
        export_110_data = project_110.export.export_tasks(binary_mask=True)
        if export_110_data:
            logging.warning(f">>>>>>>>>>>>>>>>>>>>>>>>>EXPORTED DATA{export_110_data}")
            iterator_to_json(export_110_data, move_to_110, file)
        else:
            logging.warning(f">>>>>>>>>>>>>>>>>>>>>>>>>THERE IS NO DATA TO BE EXPORTED")

    except subprocess.CalledProcessError as e:
        logging.warning(f"Error running export script: {e.stderr}")
        exit(1)


def store_json_file():
    """Find Json and Process it"""
    storage_client = storage.Client()
    bucket_110 = storage_client.bucket("lungreds-1st")

    try:
        blob = bucket_110.blob(f"json/{destination_110_json_name}")
        blob.upload_from_filename(export_file_110)
        print(f"File from {export_file_110} uploaded to bucket lung-rads-50B.")

    except Exception as e:
        print(f"Failed to upload file to {export_file_110}. Error: {e}")
        raise


def transform_data_from_bucket_lungrads_test():
    """
    Downloads data from a source bucket, transforms it, and saves it to a destination bucket.
    """
    source_bucket_name = "lungreds-1st"
    source_blob_name = f"json/{destination_110_json_name}"

    destination_bucket_name = "lungreds-1st"
    destination_blob_name = f"csv/{destination_110_csv_name}"

    storage_client = storage.Client()

    try:
        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)
        json_data_bytes = source_blob.download_as_bytes()

        json_data_string = json_data_bytes.decode("utf-8")
        raw_data = json.loads(json_data_string)

        df = create_a_data_frame(raw_data)
        new_df = recreate_new_dataframe(df)

        csv_buffer = io.StringIO()
        new_df.to_csv(csv_buffer, index=False)
        csv_string = csv_buffer.getvalue()

        destination_bucket = storage_client.bucket(destination_bucket_name)
        destination_blob = destination_bucket.blob(destination_blob_name)
        destination_blob.upload_from_string(csv_string, content_type="text/csv")

        print(f"Data transformed and uploaded to {destination_blob_name} successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    logging.warning("Starting the daily export...")

    run_organization()

    store_json_file()

    transform_data_from_bucket_lungrads_test()
