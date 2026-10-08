/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Money as integers. Amounts are carried in the minor unit of their currency and converted to US
// cents with integer rates, so a batch always adds up to the same total.

import type { Currency } from "./types.js";

export const CURRENCIES: readonly Currency[] = ["USD", "EUR", "GBP", "JPY"];

/** US dollars per one major unit of each currency. */
export type FxRates = Readonly<Record<Currency, number>>;

export const DEFAULT_FX_RATES: FxRates = { USD: 1, EUR: 1.08, GBP: 1.27, JPY: 0.0067 };

const MINOR_DIGITS: Readonly<Record<Currency, number>> = { USD: 2, EUR: 2, GBP: 2, JPY: 0 };
const RATE_SCALE = 1_000_000;

export function minorPerMajor(currency: Currency): number {
  return 10 ** MINOR_DIGITS[currency];
}

export function toUsdCents(amountMinor: number, currency: Currency, rates: FxRates): number {
  const rate = Math.round(rates[currency] * RATE_SCALE);
  return Math.round((amountMinor * rate * 100) / (RATE_SCALE * minorPerMajor(currency)));
}

export function fromUsdCents(usdCents: number, currency: Currency, rates: FxRates): number {
  const rate = Math.round(rates[currency] * RATE_SCALE);
  return Math.round((usdCents * minorPerMajor(currency) * RATE_SCALE) / (rate * 100));
}

/** Reads the `storefront.fx_rates` Variable. Currencies it leaves out keep the default rate. */
export function parseFxRates(raw: string | null): FxRates {
  if (raw === null || raw.trim() === "") return DEFAULT_FX_RATES;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`Variable storefront.fx_rates is not valid JSON: ${String(error)}`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error('Variable storefront.fx_rates must be a JSON object such as {"EUR": 1.08}');
  }
  const rates: Record<Currency, number> = { ...DEFAULT_FX_RATES };
  for (const currency of CURRENCIES) {
    const value = (parsed as Record<string, unknown>)[currency];
    if (value === undefined) continue;
    if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
      throw new Error(
        `Variable storefront.fx_rates has an invalid ${currency} rate: ${String(value)}`,
      );
    }
    rates[currency] = value;
  }
  return rates;
}

export function sum(values: Iterable<number>): number {
  let total = 0;
  for (const value of values) total += value;
  return total;
}

export function formatUsd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  const dollars = Math.floor(abs / 100).toLocaleString("en-US");
  return `${sign}$${dollars}.${String(abs % 100).padStart(2, "0")}`;
}

export function formatMinor(amountMinor: number, currency: Currency): string {
  const digits = MINOR_DIGITS[currency];
  const major = (amountMinor / minorPerMajor(currency)).toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
  return `${major} ${currency}`;
}

export function toDollars(cents: number): number {
  return cents / 100;
}
