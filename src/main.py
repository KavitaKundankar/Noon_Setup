import os
from dotenv import load_dotenv
from logger_config import logger

from db_connection.rabbit_inbound_loader import load_inbound_credentials
from inbound.rabbit_inbound import RabbitMQInbound
from parser.parser_mailbody import NoonReportParser
from mapping.mapping_mailbody import NoonReportMapper


load_dotenv()

def main():
    api_key = os.getenv("GEMINI_API_KEY")

    rabbit_cfg = load_inbound_credentials()

    parser = NoonReportParser(api_key=api_key)
    mapper = NoonReportMapper()

    worker = RabbitMQInbound(
        rabbit_cfg,
        parser=parser,
        mapper=mapper
    )

    worker.start_worker()

if __name__ == "__main__":
    main()





















# def main():

#     api_key = os.getenv("GEMINI_API_KEY")

#     # STEP 1 — Load Rabbit credentials from DB2
#     rabbit_cfg = load_inbound_credentials()

#     # STEP 2 — Connect to RabbitMQ and fetch message
#     inbound = RabbitMQInbound(rabbit_cfg)
#     message = inbound.fetch()
#     # print(f"message : {message}")

#     # STEP 3 — Fetch IMO data from DB1
#     vessel_imo = get_imo(message)
#     mail_body = str(message['body'])
#     tenant = message['tenant']

#     # STEP 4 — Mailbody Parsing
#     parser = NoonReportParser(api_key=api_key)
#     parsed_mail = parser.parse(mail_body, tenant)

#     # STEP 5 — Mapping
#     mapper = NoonReportMapper()
#     mapped_mail = mapper.map(parsed_mail, tenant)

#     logger.info("Main completed.")
#     return mapped_mail


# if __name__ == "__main__":
#     main()

