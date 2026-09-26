ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 tzdata jq \
    && python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir openai rich python-dotenv

# The generator looks for ai_rules.txt one level above tools/
WORKDIR /app
COPY ai_rules.txt ./
COPY tools/MULTI-PROVIDER_output_generator_API_v6.py ./tools/
COPY service.py ui.html ./

COPY run.sh /
RUN chmod a+x /run.sh
CMD [ "/run.sh" ]
