import json
import logging
from plugins.c2c_pb2 import NFCData
from plugins.c2s_pb2 import ServerData
import asyncio
from colorama import init, Fore, Style

# Initialize colorama
init(autoreset=True)

# Configure structured logging with a custom handler for colored output
logger = logging.getLogger("mod_log")
logger.setLevel(logging.INFO)

# Custom logging handler that adds colors based on log level
class ColorHandler(logging.StreamHandler):
    def emit(self, record):
        message = self.format(record)
        if record.levelno == logging.INFO:
            message = f"{Fore.GREEN}{message}{Style.RESET_ALL}"
        elif record.levelno == logging.ERROR:
            message = f"{Fore.RED}{message}{Style.RESET_ALL}"
        elif record.levelno == logging.DEBUG:
            message = f"{Fore.BLUE}{message}{Style.RESET_ALL}"
        elif record.levelno == logging.WARNING:
            message = f"{Fore.YELLOW}{message}{Style.RESET_ALL}"
        print(message)

# Set up the custom color handler
color_handler = ColorHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
color_handler.setFormatter(formatter)
logger.addHandler(color_handler)


def format_data(nfc_data):
    """
    Formats the NFC data into a structured dictionary. Handles errors gracefully
    to ensure that the function does not crash on malformed data.
    """
    try:
        if not nfc_data:
            return ""

        parsed_data = NFCData()
        parsed_data.ParseFromString(nfc_data)

        letter = "C" if parsed_data.data_source == NFCData.CARD else "R"
        initial = "(initial) " if parsed_data.data_type == NFCData.INITIAL else ""
        data_hex = bytes(parsed_data.data).hex()

        return {
            "source": letter,
            "initial": initial.strip(),
            "data": data_hex
        }

    except Exception as e:
        logger.error(f"Error formatting NFC data: {e}")
        return {"error": str(e)}


async def handle_data(log, data, state):
    """
    Asynchronously handles data received from the server. Parses the data using
    protocol buffers, formats it, and logs the structured information. Integrates
    with an external service if needed.
    """
    try:
        server_message = ServerData()
        server_message.ParseFromString(data)

        formatted_data = format_data(server_message.data)

        log_entry = {
            "opcode": ServerData.Opcode.Name(server_message.opcode),
            "data": formatted_data
        }

        # Log the structured data
        log_message = json.dumps(log_entry)
        log(log_message)  # Default log level is INFO

        # Asynchronously log to an external service if needed (e.g., monitoring system)
        await log_to_external_service(log_entry)

    except Exception as e:
        log(f"Error handling data: {e}", level="ERROR")
        return {"error": str(e)}

    return data


async def log_to_external_service(log_entry):
    """
    Example of an async function that logs the data to an external monitoring system.
    Replace this with actual logic for integrating with an API, database, or other service.
    """
    try:
        # Simulate async logging operation (e.g., an API call)
        await asyncio.sleep(0.1)  # Simulated delay for async logging
        logger.info(f"Logged to external service: {log_entry}")

    except Exception as e:
        logger.error(f"Failed to log to external service: {e}")


def log(message, level="INFO"):
    """
    Custom log function that logs with different levels and colors.
    """
    if level == "INFO":
        logger.info(message)
    elif level == "ERROR":
        logger.error(message)
    elif level == "DEBUG":
        logger.debug(message)
    elif level == "WARNING":
        logger.warning(message)
    else:
        logger.info(message)
