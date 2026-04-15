# Build with the repository root as context:
# docker build -f libs/eyvan/Dockerfile -t eyvan:latest .
FROM node:22-alpine AS dashboard
WORKDIR /dashboard
COPY libs/eyvan/dashboard/package*.json ./
RUN npm ci
COPY libs/eyvan/dashboard/ ./
ARG VITE_EYVAN_OIDC_ISSUER
ARG VITE_EYVAN_OIDC_CLIENT_ID
RUN npm run build

FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:stable AS runtime
COPY --from=ghcr.io/astral-sh/uv:0.9.21 /uv /uvx /bin/
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    EYVAN_DASHBOARD_DIR=/app/dashboard/dist
WORKDIR /app
# Pinned dependency closure exported from the workspace uv.lock; refresh with:
# uv export --package eyvan --no-dev --no-emit-workspace --locked -o libs/eyvan/requirements-docker.txt
COPY libs/eyvan/requirements-docker.txt ./requirements-docker.txt
RUN uv venv "$VIRTUAL_ENV" && \
    uv pip install --require-hashes -r requirements-docker.txt
COPY libs/eyvan/ ./
RUN uv pip install --no-deps .
COPY --from=dashboard /dashboard/dist /app/dashboard/dist
EXPOSE 8080
ENTRYPOINT ["eyvan-server"]
CMD ["--host", "0.0.0.0", "--port", "8080"]
