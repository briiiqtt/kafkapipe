FROM docker.elastic.co/kibana/kibana:7.6.1

# 플러그인 디렉토리 생성
RUN mkdir -p x-pack/plugins/translations/translations

# 한국어 JSON 복사
COPY ./ko-KR.json /usr/share/kibana/x-pack/plugins/translations/translations/ko-KR.json

# .i18nrc.json 복사
COPY ./.i18nrc.json /usr/share/kibana/x-pack/.i18nrc.json
