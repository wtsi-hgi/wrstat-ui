/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *   Michael Woolnough <mw31@sanger.ac.uk>
 *   Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

import { useEffect, useRef, useState } from "react";
import { asDaysAgo, formatBytes, formatNumber } from "./format";
import { firstRender, restoring } from "./state";

type ScatterParams = {
  data: Data[];
  width: number;
  height: number;
  logX?: boolean;
  logY?: boolean;
  minX: number;
  maxX: number;
  minY: number;
  maxY: number;
  setLimits: (
    minSize: number,
    maxSize: number,
    minDate: number,
    maxDate: number
  ) => void;
  previewLimits: (
    minSize: number,
    maxSize: number,
    minDate: number,
    maxDate: number
  ) => void;
  isSelected: (u: Data) => boolean;
};

type Data = {
  UsageSize: number;
  Mtime: string;
};

let scatterKey = 0;

const minDaysAgo = (date: string) => {
    const daysAgo = asDaysAgo(date);
    if (daysAgo < 0) {
      return 0;
    }

    return daysAgo;
  },
  paddingXL = 80,
  paddingXR = 10,
  paddingYT = 1,
  paddingYB = 65,
  innerPadding = 10,
  logRatio = (value: number, min: number, max: number) =>
    min
      ? Math.log(value / min) / Math.log(max / min)
      : Math.log(value + 1) / Math.log(max + 1),
  logRatioToValue = (r: number, min: number, max: number) =>
    Math.pow(Math.E, (min ? Math.log(max / min) : Math.log(max + 1)) * r) *
      (min || 1) -
    (min || 1),
  ScatterComponent = ({
    data,
    width,
    height,
    logX = false,
    logY = false,
    setLimits,
    previewLimits,
    minX,
    maxX,
    minY,
    maxY,
    isSelected,
  }: ScatterParams) => {
    const graphWidth = width - paddingXL - paddingXR - 2 * innerPadding,
      graphHeight = height - paddingYT - paddingYB - 2 * innerPadding,
      sizeToY = (size: number, log = logY) =>
        paddingYT + innerPadding + (log ? logSizeToY : nonLogSizeToY)(size),
      dateToX = (days: number, log = logX) =>
        paddingXL + innerPadding + (log ? logDateToX : nonLogDateToX)(days),
      nonLogSizeToY = (size: number) => yScale * (maxSize - size),
      nonLogDateToX = (days: number) => xScale * (days - minDate),
      logSizeToY = (size: number) =>
        graphHeight - graphHeight * logRatio(size, minSize, maxSize),
      logDateToX = (days: number) =>
        graphWidth * logRatio(days, minDate, maxDate),
      fractionToSize = (f: number) =>
        Math.round((logY ? logFractionToSize : nonLogFractionToSize)(f)),
      fractionToDate = (f: number) =>
        Math.round((logX ? logFractionToDate : nonLogFractionToDate)(f)),
      nonLogFractionToSize = (f: number) => minSize + f * (maxSize - minSize),
      nonLogFractionToDate = (f: number) => minDate + f * (maxDate - minDate),
      logFractionToSize = (f: number) =>
        minSize + logRatioToValue(f, minSize, maxSize),
      logFractionToDate = (f: number) =>
        minDate + logRatioToValue(f, minDate, maxDate),
      [highlightCoords, setHighlightCoords] = useState<
        null | [number, number, number, number]
      >(null);
    const canvasRef = useRef<HTMLCanvasElement | null>(null);

    let minSize = Infinity,
      maxSize = -Infinity,
      minDate = Infinity,
      maxDate = -Infinity;

    for (const d of data) {
      if (d.UsageSize < minSize) {
        minSize = d.UsageSize;
      }

      if (d.UsageSize > maxSize) {
        maxSize = d.UsageSize;
      }

      const daysAgo = minDaysAgo(d.Mtime);

      if (daysAgo < minDate) {
        minDate = daysAgo;
      }

      if (daysAgo > maxDate) {
        maxDate = daysAgo;
      }
    }

    const orderMax = Math.pow(10, Math.max(Math.floor(Math.log10(maxDate)), 1)),
      orderMin = Math.pow(10, Math.max(Math.floor(Math.log10(minDate)), 1));

    minDate = orderMin * Math.floor(minDate / orderMin);
    maxDate = orderMax * Math.ceil(maxDate / orderMax);
    minSize = 100 * Math.pow(2, Math.floor(Math.log2(minSize / 100)));
    maxSize = 100 * Math.pow(2, Math.ceil(Math.log2(maxSize / 100)));

    const xScale = graphWidth / (maxDate - minDate),
      yScale = graphHeight / (maxSize - minSize);

    // ---- Draw points on canvas ----
    useEffect(() => {
      if (!canvasRef.current || data.length === 0) return;

      const ctx = canvasRef.current.getContext("2d");
      if (!ctx) return;

      ctx.clearRect(0, 0, canvasRef.current.width, canvasRef.current.height);

      const radius = 3;
      for (const d of data) {
        const x = dateToX(minDaysAgo(d.Mtime));
        const y = sizeToY(d.UsageSize);

        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        ctx.fillStyle = isSelected(d) ? "red" : "black";
        ctx.fill();
      }
    }, [data, dateToX, sizeToY, isSelected]);

    // ---- Handle click detection ----
    useEffect(() => {
      if (!canvasRef.current) return;
      const canvas = canvasRef.current;

      const handleClick = (e: MouseEvent) => {
        const rect = canvas.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const mouseY = e.clientY - rect.top;

        let hit: Data | null = null;
        let minDist = Infinity;

        for (const d of data) {
          const x = dateToX(minDaysAgo(d.Mtime));
          const y = sizeToY(d.UsageSize);
          const dist = Math.hypot(mouseX - x, mouseY - y);
          if (dist < 5 && dist < minDist) {
            hit = d;
            minDist = dist;
          }
        }

        if (hit) {
          setLimits(
            hit.UsageSize,
            hit.UsageSize,
            asDaysAgo(hit.Mtime),
            asDaysAgo(hit.Mtime)
          );
          setHighlightCoords(null);
        } else {
          setHighlightCoords(null);
          setLimits(-Infinity, Infinity, -Infinity, Infinity);
        }
      };

      canvas.addEventListener("click", handleClick);
      return () => canvas.removeEventListener("click", handleClick);
    }, [data, dateToX, sizeToY, setLimits]);

    // ---- Cursor hover over points ----
    useEffect(() => {
      if (!canvasRef.current) return;
      const canvas = canvasRef.current;

      const handleMouseMove = (e: MouseEvent) => {
        const rect = canvas.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const mouseY = e.clientY - rect.top;

        let hovering = false;
        for (const d of data) {
          const x = dateToX(minDaysAgo(d.Mtime));
          const y = sizeToY(d.UsageSize);
          if (Math.hypot(mouseX - x, mouseY - y) <= 3) {
            hovering = true;
            break;
          }
        }

        canvas.style.cursor = hovering ? "pointer" : "crosshair";
      };

      canvas.addEventListener("mousemove", handleMouseMove);
      return () => canvas.removeEventListener("mousemove", handleMouseMove);
    }, [data, dateToX, sizeToY]);

    // ---- Drag-to-zoom selection ----
    const onDrag = (e: React.MouseEvent<SVGSVGElement>) => {
      if (e.button !== 0) return;

      const rect = e.currentTarget.getBoundingClientRect();
      const graphLeft = rect.left + paddingXL;
      const graphTop = rect.top + paddingYT;
      const startX = e.clientX - graphLeft;
      const startY = e.clientY - graphTop;

      const mousemove = (ev: MouseEvent, cb = previewLimits) => {
        const x = ev.clientX - graphLeft;
        const y = ev.clientY - graphTop;

        const minXSel = Math.max(Math.min(x, startX), 0);
        const maxXSel = Math.min(
          Math.max(x, startX),
          graphWidth + 2 * innerPadding
        );
        const minYSel = Math.max(Math.min(y, startY), 0);
        const maxYSel = Math.min(
          Math.max(y, startY),
          graphHeight + 2 * innerPadding
        );

        if (minXSel === maxXSel || minYSel === maxYSel) {
          setHighlightCoords(null);
          cb(-Infinity, Infinity, -Infinity, Infinity);
          return;
        }

        setHighlightCoords([
          minXSel,
          maxXSel - minXSel,
          minYSel,
          maxYSel - minYSel,
        ]);

        const fMinX = Math.max(0, minXSel - innerPadding) / graphWidth;
        const fMaxX = Math.min(graphWidth, maxXSel - innerPadding) / graphWidth;
        const fMinY =
          Math.max(0, graphHeight - maxYSel + innerPadding) / graphHeight;
        const fMaxY =
          Math.min(graphHeight, graphHeight - minYSel + innerPadding) /
          graphHeight;

        const minDaysAgo = fractionToDate(fMinX);
        const maxDaysAgo = fractionToDate(fMaxX);
        const minFileSize = fractionToSize(fMinY);
        const maxFileSize = fractionToSize(fMaxY);

        cb(minFileSize, maxFileSize, minDaysAgo, maxDaysAgo);

        canvasRef.current!.style.cursor = "grabbing";
      };

      const mouseup = (ev: MouseEvent) => {
        if (ev.button !== 0) return;
        mousemove(ev, setLimits);
        setHighlightCoords(null);
        canvasRef.current!.style.cursor = "crosshair";

        window.removeEventListener("mousemove", mousemove);
        window.removeEventListener("mouseup", mouseup);
        window.removeEventListener("keydown", keydown);
      };
      const keydown = (ev: KeyboardEvent) => {
        if (ev.key === "Escape") {
          const x = dateToX(minX),
            y = sizeToY(maxY),
            width = dateToX(maxX) - x,
            height = sizeToY(minY) - y;

          setHighlightCoords([x - paddingXL, width, y - paddingYT, height]);
          setLimits(minY, maxY, minX, maxX);

          window.removeEventListener("mousemove", mousemove);
          window.removeEventListener("mouseup", mouseup);
          window.removeEventListener("keydown", keydown);
        }
      };
      if (
        startX < 0 ||
        startX > graphWidth + 2 * innerPadding ||
        startY < 0 ||
        startY > graphHeight + 2 * innerPadding
      ) {
        return;
      }

      window.addEventListener("mousemove", mousemove);
      window.addEventListener("mouseup", mouseup);
      window.addEventListener("keydown", keydown);
    };

    const dataStr = JSON.stringify(data);

    useEffect(() => {
      if (data.length === 0) {
        return;
      }

      const x = dateToX(minX),
        y = sizeToY(maxY),
        width = dateToX(maxX) - x,
        height = sizeToY(minY) - y;

      setHighlightCoords([x - paddingXL, width, y - paddingYT, height]);
    }, [minX, minY, maxX, maxY, width, height]);

    useEffect(() => {
      if (firstRender || restoring) {
        return;
      }

      setHighlightCoords(null);
      setLimits(-Infinity, Infinity, -Infinity, Infinity);
    }, [dataStr]);

    if (data.length === 0) {
      return (
        <svg
          id="scatter"
          xmlns="http://www.w3.org/2000/svg"
          width={width}
          height={height}
          viewBox={`0 0 ${width} ${height}`}
        >
          <rect
            width={width}
            height={height}
            stroke="currentColor"
            fill="none"
          />
          <text
            fill="currentColor"
            textAnchor="middle"
            x={width / 2}
            y={height / 2}
          >
            —No Data—
          </text>
        </svg>
      );
    }

    return (
      <svg
        id="scatter"
        key={`scatter_${++scatterKey}`}
        xmlns="http://www.w3.org/2000/svg"
        width={width}
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        onMouseDown={onDrag}
      >
        <rect
          rx={4}
          className="back"
          x={paddingXL}
          y={paddingYT}
          width={graphWidth + 2 * innerPadding}
          height={graphHeight + 2 * innerPadding}
          style={{ fill: "var(--graphBack, #ddd)" }}
          stroke="currentColor"
        />
        {Array.from({ length: 6 }, (_, n) => (
          <line
            key={`scatter_hl_${n}`}
            x1={dateToX(nonLogFractionToDate(0), false) - innerPadding}
            x2={dateToX(nonLogFractionToDate(1), false) + innerPadding}
            y1={sizeToY(nonLogFractionToSize(n / 5), false)}
            y2={sizeToY(nonLogFractionToSize(n / 5), false)}
            className="graphLines"
          />
        ))}
        {Array.from({ length: 6 }, (_, n) => (
          <text
            key={`scatter_ht_${n}`}
            x={dateToX(nonLogFractionToDate(0), false) - innerPadding - 5}
            y={
              Math.max(sizeToY(nonLogFractionToSize(n / 5), false), paddingYT) +
              5
            }
            fill="currentColor"
            textAnchor="end"
          >
            {formatBytes(fractionToSize(n / 5))}
          </text>
        ))}
        {Array.from({ length: 6 }, (_, n) => (
          <line
            key={`scatter_vl_${n}`}
            x1={dateToX(nonLogFractionToDate(n / 5), false)}
            x2={dateToX(nonLogFractionToDate(n / 5), false)}
            y1={sizeToY(nonLogFractionToSize(1), false) - innerPadding}
            y2={sizeToY(nonLogFractionToSize(0), false) + innerPadding}
            className="graphLines"
          />
        ))}
        {Array.from({ length: 6 }, (_, n) => (
          <text
            key={`scatter_vt_${n}`}
            x={-10}
            y={20}
            transform={`translate(${dateToX(
              nonLogFractionToDate(n / 5),
              false
            )} ${sizeToY(nonLogFractionToSize(0), false)}) rotate(-45)`}
            fill="currentColor"
            textAnchor="end"
          >
            {formatNumber(fractionToDate(n / 5))}
          </text>
        ))}
        <foreignObject x={0} y={0} width={width} height={height}>
          <canvas
            ref={canvasRef}
            width={width}
            height={height}
            style={{ width: "100%", height: "100%", cursor: "crosshair" }}
          />
        </foreignObject>
        {highlightCoords &&
        highlightCoords.every((v) => v !== -Infinity && v !== Infinity) ? (
          <rect
            className="back"
            x={highlightCoords[0] + paddingXL}
            width={highlightCoords[1]}
            y={highlightCoords[2] + paddingYT}
            height={highlightCoords[3]}
            fill="#9cf"
            fillOpacity={0.25}
            stroke="#036"
            strokeOpacity={0.25}
          />
        ) : (
          []
        )}
        <text
          x={paddingXL + (width - paddingXL - paddingXR) / 2}
          y={height - 5}
          textAnchor="middle"
          fill="currentColor"
        >
          Last Modified (Days)
        </text>
      </svg>
    );
  };

export default ScatterComponent;
