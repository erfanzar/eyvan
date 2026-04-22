/*
 * Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

// The control plane is not always on 8080: `eyvan runtime` picks a per-profile
// port, and a developer commonly has more than one running. Hardcoding the
// target sent every request to whichever server happened to own 8080, which
// renders as an empty dashboard rather than an error -- the worst failure mode,
// because it looks like the fleet is idle.
//
// `loadEnv` with an empty prefix reads the real process environment as well as
// .env files, so this works both as `EYVAN_DASHBOARD_API=... npm run dev` and as
// a checked-in .env.local. The cwd is passed as "." rather than `process.cwd()`
// so the config needs no Node type definitions.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  const controlPlane = env.EYVAN_DASHBOARD_API || "http://127.0.0.1:8080";
  return {
    plugins: [react()],
    server: {
      port: 4173,
      proxy: {
        "/api": controlPlane,
      },
    },
    preview: {
      port: 4174,
    },
  };
});
