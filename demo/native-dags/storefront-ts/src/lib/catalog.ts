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

// The storefront catalog the synthetic feeds draw from.

export interface Product {
  sku: string;
  name: string;
  category: string;
  priceUsdCents: number;
  /** Relative share of order lines. */
  popularity: number;
}

const LEAD_TIME_DAYS: Readonly<Record<string, number>> = {
  apparel: 21,
  footwear: 28,
  home: 14,
  electronics: 35,
  beauty: 10,
};

const ROWS: readonly (readonly [string, string, string, number, number])[] = [
  ["TEE-001", "Classic Crew Tee", "apparel", 2400, 10],
  ["TEE-002", "Heavyweight Pocket Tee", "apparel", 2900, 7],
  ["HDY-001", "Everyday Hoodie", "apparel", 5900, 8],
  ["HDY-002", "Zip Fleece Hoodie", "apparel", 6900, 4],
  ["JKT-001", "Rain Shell Jacket", "apparel", 11900, 3],
  ["JKT-002", "Quilted Vest", "apparel", 8900, 2],
  ["JNS-001", "Straight Fit Jeans", "apparel", 7900, 6],
  ["SOC-001", "Merino Sock 3-Pack", "apparel", 2200, 9],
  ["CAP-001", "Waxed Canvas Cap", "apparel", 2800, 4],
  ["SNK-001", "Court Sneaker", "footwear", 9900, 6],
  ["SNK-002", "Trail Runner", "footwear", 13900, 4],
  ["BOT-001", "Leather Chelsea Boot", "footwear", 17900, 2],
  ["SND-001", "Slide Sandal", "footwear", 3500, 3],
  ["MUG-001", "Stoneware Mug", "home", 1800, 8],
  ["CND-001", "Soy Candle", "home", 2600, 7],
  ["BLK-001", "Wool Throw Blanket", "home", 8900, 3],
  ["LMP-001", "Desk Lamp", "home", 5400, 3],
  ["PLW-001", "Linen Pillow Cover", "home", 3200, 4],
  ["KTL-001", "Gooseneck Kettle", "home", 6900, 3],
  ["EAR-001", "Wireless Earbuds", "electronics", 12900, 6],
  ["EAR-002", "Noise Cancelling Headphones", "electronics", 27900, 3],
  ["WCH-001", "Fitness Watch", "electronics", 19900, 3],
  ["SPK-001", "Portable Speaker", "electronics", 8900, 4],
  ["CHG-001", "65W USB-C Charger", "electronics", 3900, 8],
  ["PWR-001", "20000mAh Power Bank", "electronics", 4900, 5],
  ["TAB-001", "10 inch Tablet", "electronics", 32900, 1],
  ["LAP-001", "Ultralight Laptop 14", "electronics", 89900, 1],
  ["CAM-001", "Action Camera", "electronics", 24900, 2],
  ["SRM-001", "Vitamin C Serum", "beauty", 3400, 7],
  ["MOI-001", "Daily Moisturizer", "beauty", 2800, 8],
  ["SUN-001", "SPF 50 Sunscreen", "beauty", 1900, 9],
  ["LIP-001", "Lip Balm 4-Pack", "beauty", 1200, 10],
  ["SHM-001", "Shampoo Bar", "beauty", 1400, 6],
  ["PRF-001", "Eau de Parfum 50ml", "beauty", 8200, 2],
  ["GFT-025", "Gift Card 25", "home", 2500, 3],
  ["GFT-050", "Gift Card 50", "home", 5000, 2],
];

export const PRODUCTS: readonly Product[] = ROWS.map(
  ([sku, name, category, priceUsdCents, popularity]) => ({
    sku,
    name,
    category,
    priceUsdCents,
    popularity,
  }),
);

export const PRODUCTS_BY_SKU: ReadonlyMap<string, Product> = new Map(
  PRODUCTS.map((p) => [p.sku, p]),
);

export function leadTimeDays(category: string): number {
  return LEAD_TIME_DAYS[category] ?? 21;
}
