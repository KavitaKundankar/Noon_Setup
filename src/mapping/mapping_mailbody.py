import json
import re
import os
from datetime import datetime
from logger_config import logger
from config import BASE_DIR
from db_connection.mapping_loader import get_standard_keys
from db_connection.save_unmapped import merge_unmapped_keys


class NoonReportMapper:
    def __init__(self):
        pass

    def map(self, parsed_mail: dict, tenant: str, imo : str, name : str):

        try:
            final_mapping = {}
            unmapped={}
            standard_data = {}

            standard_data = get_standard_keys(tenant)

            # Perform mapping
            for key, value in parsed_mail.items():

                if value is None or value == "" or value == "0" or value == 0:           # To skip null values
                    continue

                if key in standard_data:
                    mapped_key = standard_data[key]
                    final_mapping[mapped_key] = value

                else :
                    unmapped[key] = value
   
            self.unmaped_save(unmapped, tenant)
            merge_unmapped_keys(unmapped, tenant)

            logger.error(f"Saved Unmapped keys for tenant {tenant} : {unmapped}")

            final_mapping.update(unmapped)

            self.save(final_mapping, tenant)

            logger.info(f"Mapping done for tenant : '{tenant}',  IMO : '{imo}' vessel : '{name}'")
            
            return final_mapping

        except Exception as e:
            logger.error(f"Error in mapping. No mapping was saved for tenant : '{tenant}', IMO : '{imo}', vessel : '{name}' : {e}")
            return {}
        

    def save(self, data: dict, tenant: str):
        try:
            output_dir = os.path.join(BASE_DIR, "mapping", "mapped_outputs")
            os.makedirs(output_dir, exist_ok=True)

            filename = os.path.join(
                output_dir,
                f"mapped_noon_report_{tenant}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )

            with open(filename, "w") as f:
                json.dump(data, f, indent=4)

            logger.info(f"Output saved at: {filename}")

        except Exception as e:
            logger.error(f"Error saving mapped file: {str(e)}")


    def unmaped_save(self, unmapped: dict, tenant: str):

        # Directory where new keys are saved
        dir_path = os.path.join(BASE_DIR, "mapping","Mapping_metadata","new_generated_keys")
        os.makedirs(dir_path, exist_ok=True)

        # File path inside that directory
        filename = os.path.join(
            dir_path,
            f"{tenant}_generated_keys.json"
        )

        # If file doesn't exist → create empty JSON object
        if not os.path.exists(filename):
            with open(filename, "w") as f:
                json.dump({}, f, indent=4)

        # Read existing JSON
        with open(filename, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}   # if file is empty or corrupted

        # Add new unmapped keys
        data.update(unmapped)

        # Save updated JSON
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)

















# path = os.path.join(BASE_DIR, "mapping","json_mappings", f"{tenant}_mapping.json")

# if not os.path.exists(path):
#     logger.error(f"Mapping file not found for tenant: {tenant}")
#     return {}

# with open(path, "r") as f:
#     standard_data = json.load(f)