import json
import re
import os
from datetime import datetime
from logger_config import logger
from config import BASE_DIR
from db_connection.mapping_loader import get_standard_keys


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
                if key in standard_data:
                    mapped_key = standard_data[key]
                    final_mapping[mapped_key] = value

                else :
                    unmapped[key] = value

            logger.error(f"Unmapped keys for tenant {tenant} : {unmapped}")

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
















# path = os.path.join(BASE_DIR, "mapping","json_mappings", f"{tenant}_mapping.json")

# if not os.path.exists(path):
#     logger.error(f"Mapping file not found for tenant: {tenant}")
#     return {}

# with open(path, "r") as f:
#     standard_data = json.load(f)