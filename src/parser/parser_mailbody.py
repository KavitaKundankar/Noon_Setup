import google.generativeai as genai
import json, re
import os
import yaml
from datetime import datetime
from logger_config import logger
from config import BASE_DIR
from db_connection.prompt_loader import get_tenant_prompt

class NoonReportParser:

    def __init__(self, api_key):

        genai.configure(api_key=api_key)

        self.model = genai.GenerativeModel("models/gemini-2.5-flash-lite")
        

    def parse(self, body, tenant):

        standard_prompt, tenant_prompt, parsed_keys = get_tenant_prompt(tenant)

        # Build final prompt safely
        final_prompt = ""

        if standard_prompt:
            final_prompt += f"{standard_prompt}\n\n"

        if tenant_prompt:
            final_prompt += f"{tenant_prompt}\n\n"

        if parsed_keys:
            final_prompt += f"{parsed_keys}\n\n"

        final_prompt += f"EMAIL CONTENT:\n{body}"

        response = self.model.generate_content(final_prompt)
        logger.info(f"Token usage: {response.usage_metadata}")

        cleaned = re.sub(r"^```json\s*|\s*```$", "", response.text.strip())

        self.save(cleaned, tenant)

        parsed_mail = json.loads(cleaned)

        try:
            parsed_mail = json.loads(cleaned)
        except Exception as e:
            logger.error(f"Invalid JSON in save(): {e}\nDATA={cleaned}")

        return parsed_mail

    def save(self, data, tenant):
        # Ensure output directory exists
        output_dir = os.path.join(BASE_DIR, "parser/parsed_outputs")
        os.makedirs(output_dir, exist_ok=True)

        filename = os.path.join(
            output_dir,
            f"noon_report_{tenant}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        # Save JSON to file
        with open(filename, "w") as f:
            json.dump(json.loads(data), f, indent=4)

        logger.info(f"Output saved: {filename}")
