#!/bin/bash
set -euo pipefail

echo "Generating access times report..."

module load HGI/common/wrstat-ui

programmes=(
    "humgen"
    "gengen"
    "cellgen"
    "tol"
    "casm"
    "pam"
    "gsu"
);

intervals=(
    "3Y"
    "5Y"
    "7Y"
    ""
);

output_csv="Programme,Area,3Y (GB),5Y (GB),7Y (GB),Total Size(GB)\n"

# Get all team/project directories
dirpaths=$(wrstat-ui where -d /lustre/ --splits 2)
progdirs=()

for dir in $dirpaths; do
    prog=$(basename "$dir")
    for p in "${programmes[@]}"; do
        if [[ "$prog" == "$p" ]]; then
            for d in "$dir"/teams* "$dir"/projects*; do
                if [ -d "$d" ]; then
                    progdirs+=("$d")
                fi
            done
        fi
    done
done

echo "Found ${#progdirs[@]} directories to analyze."

# Retrieve subdir info
for subdir in "${progdirs[@]}"; do
    for area_dir in "$subdir"/*; do
        if [ -d "$area_dir" ]; then
            area=$(basename "$area_dir")
            programme=$(basename $(dirname $(dirname "$area_dir")))
            echo "Processing $subdir - $programme - $area"

            line="${programme},${area}"

            for time in "${intervals[@]}"; do
                json_output=$(wrstat-ui where -j -d "$area_dir" --unused "$time" --splits 0 --show_ug)
                while IFS=$',' read -r size groups; do
                    if [[ "$size" == "" ]]; then
                        line+=",0"
                        continue
                    fi
                    size_tb=$(awk -v s="$size" 'BEGIN { printf "%.4f", s / (1024^3) }')
                    line+=",$size_tb"
                done <<< "$(echo "$json_output" | jq -r '.[] |[.Size, (.Groups | join(":"))] | @csv')"
            done
            output_csv+="${line}\n"
        fi
    done
done

echo -e "$output_csv" > backup_summary_report.csv

echo "Report saved to backup_summary_report.csv"