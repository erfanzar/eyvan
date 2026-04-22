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
import { AlertTriangle, LoaderCircle, RefreshCw } from "lucide-react";

export function LoadingState() {
  return (
    <div className="page-state" role="status">
      <LoaderCircle className="spin" size={22} aria-hidden="true" />
      <span>Loading control-plane state</span>
    </div>
  );
}

interface ErrorStateProps {
  message: string;
  onRetry: () => void;
}

export function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="page-state page-state-error" role="alert">
      <AlertTriangle size={22} aria-hidden="true" />
      <strong>Unable to load Eyvan state</strong>
      <span>{message}</span>
      <button className="button button-secondary" type="button" onClick={onRetry}>
        <RefreshCw size={16} aria-hidden="true" />
        Retry
      </button>
    </div>
  );
}
