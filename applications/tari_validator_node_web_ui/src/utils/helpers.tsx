//  Copyright 2022. The Tari Project
//
//  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//  following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//  disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//  following disclaimer in the documentation and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//  products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import { toHexString } from "../routes/VN/Components/helpers";
import { ShardGroup, SubstateId, substateIdToString } from "@tari-project/typescript-bindings";

export const renderJson = (json: any) => {

  if (json && Array.isArray(json)) {
    //eslint-disable-next-line eqeqeq
    if (json.length == 32) {
      return <span className="string">"{toHexString(json)}"</span>;
    }
    return (
      <>
        [
        <ol>
          {json.map((val) => (
            <li key={val}>{renderJson(val)},</li>
          ))}
        </ol>
        ],
      </>
    );
  }

  if (typeof json === "object") {
    if (!json) {
      return <span>null</span>;
    }
    return (
      <>
        {"{"}
        <ul>
          {Object.keys(json).map((key) => (
            <li key={key}>
              <b>"{key}"</b>:{renderJson(json[key])}
            </li>
          ))}
        </ul>
        {"}"}
      </>
    );
  }

  if (typeof json === "string") return <span className="string">"{json}"</span>;
  if (typeof json === "number") return <span className="number">"{json}"</span>;
  if (typeof json === "boolean") return <span className="boolean">{json ? "true" : "false"}</span>;
  return <span className="other">{json}</span>;
};

export function fromHexString(hexString: string) {
  let res = [];
  for (let i = 0; i < hexString.length; i += 2) {
    res.push(Number("0x" + hexString.substring(i, i + 2)));
  }
  return res;
}

export function shortenSubstateId(substateId: SubstateId | null | undefined, start: number = 4, end: number = 4) {
  if (substateId === null || substateId === undefined) {
    return "";
  }
  const string = substateIdToString(substateId);
  const parts = string.split("_", 2);
  return parts[0] + "_" + shortenString(parts[1], start, end);
}

export function shortenString(string: string, start: number = 8, end: number = 8) {
  return string.substring(0, start) + "..." + string.slice(-end);
}

export function emptyRows(page: number, rowsPerPage: number, array: any[]) {
  return page > 0 ? Math.max(0, (1 + page) * rowsPerPage - array.length) : 0;
}


export function primitiveDateTimeToDate([year, dayOfTheYear, hour, minute, second, nanos]: number[]): Date {
  return new Date(year, 0, dayOfTheYear, hour, minute, second, nanos / 1000000);
}

export function primitiveDateTimeToSecs([year, dayOfTheYear, hour, minute, second, nanos]: number[]): number {
  // The datetime is in format [year, day of the year, hour, minute, second, nanos]
  return new Date(year, 0, dayOfTheYear, hour, minute, second, nanos / 1000000).valueOf() / 1000;
}

export interface Duration {
  secs: number;
  nanos: number;
}

export function displayDuration(duration: Duration) {
  if (duration.secs === 0) {
    if (duration.nanos > 1000000) {
      return `${(duration.nanos / 1000000).toFixed(2)}ms`;
    }
    if (duration.nanos > 1000) {
      return `${(duration.nanos / 1000).toFixed(2)}µs`;
    }
    return `${duration.nanos}ns`;
  }
  if (duration.secs >= 60 * 60) {
    const minutes_secs = duration.secs - Math.floor(duration.secs / 60 / 60) * 60 * 60;
    return `${(duration.secs / 60 / 60).toFixed(0)}h${Math.floor(minutes_secs / 60)}m`;
  }
  if (duration.secs >= 60) {
    const secs = duration.secs - Math.floor(duration.secs / 60) * 60;
    return `${(duration.secs / 60).toFixed(0)}m${secs.toFixed(0)}s`;
  }
  return `${duration.secs}s`;
}


export function decodeShardGroup(sg: number): ShardGroup {
  const start = sg >> 8;
  const end_inclusive = sg & 0xff;
  return { start, end_inclusive };
}