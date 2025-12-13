import os
from dotenv import load_dotenv
import google.generativeai as genai
import logging

load_dotenv()
logger = logging.getLogger(__name__)

class GeminiKeyManager:
    def __init__(self):
        self.keys = [
            os.getenv("GEMINI_API_KEY_1"),
            os.getenv("GEMINI_API_KEY_2"),
            os.getenv("GEMINI_API_KEY_3"),
        ]

        # clean None keys
        self.keys = [k for k in self.keys if k]

        if not self.keys:
            raise Exception("No API keys found in .env")

        self.current_index = 0
        self.set_key(self.keys[self.current_index])

    def set_key(self, key):
        genai.configure(api_key=key)
        logger.info(f"🔑 Using Gemini API Key #{self.current_index + 1}")

    def rotate_key(self):
        """Move to next key. Stop when exhausted."""
        self.current_index += 1

        if self.current_index >= len(self.keys):
            # All keys exhausted → STOP worker
            logger.critical("❌ All Gemini API keys exhausted. Cannot continue.")
            raise Exception("All API keys quota exceeded.")

        new_key = self.keys[self.current_index]
        self.set_key(new_key)
        logger.warning(f"🔄 Rotated to Gemini API Key #{self.current_index + 1}")

    def call_gemini(self, prompt):
        """Call Gemini with rotation, stop after last key."""
        try:
            model = genai.GenerativeModel("gemini-2.5-flash-lite")
            response = model.generate_content(prompt)
            return response.text

        except Exception as e:
            error = str(e)

            # Quota exceeded → rotate to next key
            if "429" in error or "quota" in error.lower():
                logger.error(f"⚠️ Key #{self.current_index + 1} quota exceeded.")
                self.rotate_key()
                return self.call_gemini(prompt)

            # Other errors → fail immediately
            raise e
