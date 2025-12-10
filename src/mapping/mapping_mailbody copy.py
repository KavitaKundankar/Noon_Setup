import json
import re
import os
from datetime import datetime
from logger_config import logger
from config import BASE_DIR
from .mapping_verification import build_llm_mapping_prompt


class NoonReportMapper:
    def __init__(self):
        pass

    def map(self, parsed_mail: dict, tenant: str, imo : str):

        try:
            path = os.path.join(BASE_DIR, "mapping","json_mappings", f"{tenant}_mapping.json")

            if not os.path.exists(path):
                logger.error(f"Mapping file not found for tenant: {tenant}")
                return

            with open(path, "r") as f:
                standard_data = json.load(f)

            final_mapping = {}
            unmapped={}
            final_unmapped = {}

            standard_data_keys = set(standard_data.keys())

            # Perform mapping
            for key, value in parsed_mail.items():
                if key in standard_data:
                    mapped_key = standard_data[key]
                    final_mapping[mapped_key] = value
                    standard_data_keys.discard(key)   # remove from remaining standard keys
                else:
                    unmapped[key] = value

            print("unmapped : ", unmapped)
            # Remaining standard keys that were not mapped
            standard_data2 = list(standard_data_keys)

            # If there are unmapped keys → run LLM logic
            try : 
                if unmapped:

                    llm_response = build_llm_mapping_prompt(unmapped, standard_data2)

                    llm_mapped = json.loads(llm_response)     # must be a dict

                    # Apply LLM mappings
                    final_unmapped = {}

                    for unmapped_key, suggested_standard_key in llm_mapped.items():
                        value = unmapped.get(unmapped_key)

                        if suggested_standard_key:
                            final_unmapped[suggested_standard_key] = value
                        else:
                            final_unmapped[unmapped_key] = value

                    # Merge LLM-mapped results
                    final_mapping.update(final_unmapped)

                else:
                    # No unmapped keys → just merge (does nothing if empty)
                    final_mapping.update(unmapped)

            except Exception as e:
                logger.error(f"Error in unmapped keys mapping: {str(e)}")
                final_mapping.update(unmapped)

            self.save(final_mapping, tenant)

            logger.info(f"Mapping done for tenant : {tenant}")
            
            return final_mapping

        except Exception as e:
            logger.error(f"Error in mapping no mapping has been saved for {tenant} {imo}: {str(e)}")
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

            logger.info(f"Mapped output saved at: {filename}")

        except Exception as e:
            logger.error(f"Error saving mapped file: {str(e)}")
