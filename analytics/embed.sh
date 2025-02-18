#!/bin/bash

(
	cd "src";
	head -n5 index.html | tr -d '\n	';
	echo -n "<style type=\"text/css\">";
	cat style.css | tr -d '\n	';
	echo -n "</style>";
	echo -n "<script type=\"module\">";
	jspacker -i "/$(grep "<script" index.html | sed -e 's/.*src="\([^"]*\)".*/\1/')" -n | terser -m  --module --compress pure_getters,passes=3 --ecma 2020 | tr -d '\n';
	echo -n "</script>";
	tail -n3 index.html | tr -d '\n	';
) > "index.html";
