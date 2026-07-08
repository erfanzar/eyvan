# Use the Eyvan package directory as build context.
# Standalone: docker build -f Dockerfile -t "$EYVAN_CONTROL_PLANE_REPOSITORY:$BUILD_ID" .
# Workspace:  docker build -f libs/eyvan/Dockerfile -t "$EYVAN_CONTROL_PLANE_REPOSITORY:$BUILD_ID" libs/eyvan
FROM node:22-alpine AS dashboard
WORKDIR /dashboard
COPY dashboard/package*.json ./
RUN npm ci
COPY dashboard/ ./
ARG VITE_EYVAN_OIDC_ISSUER
ARG VITE_EYVAN_OIDC_CLIENT_ID
RUN npm run build

FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:stable AS runtime
ARG EYVAN_VERSION=0.1.0
ARG EYVAN_REVISION=unknown
LABEL org.opencontainers.image.title="Eyvan control plane" \
      org.opencontainers.image.version="${EYVAN_VERSION}" \
      org.opencontainers.image.revision="${EYVAN_REVISION}" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later"
COPY --from=ghcr.io/astral-sh/uv:0.9.21 /uv /uvx /bin/
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    EYVAN_DASHBOARD_DIR=/app/dashboard/dist
WORKDIR /app
# Pinned dependency closure exported from the workspace uv.lock; refresh from
# the workspace root with:
# uv export --package eyvan --no-dev --no-emit-workspace --locked -o libs/eyvan/requirements-docker.txt
COPY requirements-docker.txt ./requirements-docker.txt
RUN uv venv "$VIRTUAL_ENV" && \
    uv pip install --require-hashes -r requirements-docker.txt
COPY pyproject.toml README.md LICENSE ./
COPY eyvan ./eyvan
RUN uv pip install --no-deps .
COPY --from=dashboard /dashboard/dist /app/dashboard/dist
EXPOSE 8080
ENTRYPOINT ["eyvan-server"]
CMD ["--host", "0.0.0.0", "--port", "8080"]
