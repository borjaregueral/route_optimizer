import os
import logging

logger = logging.getLogger(__name__)

class RoutifySessionManager:
    def __init__(self):
        self.routific_token = self.load_routific_token()

    def load_routific_token(self) -> str:
        """
        Load the Routific token from the environment variables.

        :return: Routific token as a string.
        """
        try:
            routific_token = os.getenv("ROUTIFIC_TOKEN")
            if not routific_token:
                raise ValueError("Routific token not found in environment variables.")
            logger.info("Routific token loaded successfully.")
            return routific_token
        except Exception as e:
            logger.error(f"Error loading Routific token: {str(e)}")
            raise

