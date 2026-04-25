// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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
