FROM nginx:alpine
COPY nginx.conf /etc/nginx/templates/default.conf.template
COPY index.html /usr/share/nginx/html/index.html
ENV SIDECAR_UPSTREAM=http://localhost:8900
