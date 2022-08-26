require.config({
    paths: {
        cookie: "js.cookie.min"
    }
})

define(["d3", "cookie"], function (d3, cookie) {
    var current;
    let allNodes = new Map();

    var margin = { top: 22, right: 0, bottom: 0, left: 0 },
        width = 960,
        height = 500 - margin.top - margin.bottom,
        transitioning;

    var x = d3.scale.linear()
        .domain([0, width])
        .range([0, width]);

    var y = d3.scale.linear()
        .domain([0, height])
        .range([0, height]);

    var treemap = d3.layout.treemap()
        .sort(function (a, b) { return a.value - b.value; })
        .ratio(height / width * 0.5 * (1 + Math.sqrt(5)))
        .round(false);

    var svg = d3.select("#chart").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.bottom + margin.top)
        .style("margin-left", -margin.left + "px")
        .style("margin.right", -margin.right + "px")
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
        .style("shape-rendering", "crispEdges");

    var grandparent = svg.append("g")
        .attr("class", "grandparent");

    grandparent.append("rect")
        .attr("y", -margin.top)
        .attr("width", width)
        .attr("height", margin.top);

    grandparent.append("text")
        .attr("x", width - 6)
        .attr("y", 6 - margin.top)
        .attr("text-anchor", "end")
        .attr("dy", ".75em");

    let hash = window.location.hash.substring(1);
    let hasher = new URL('https://hasher.com')
    hasher.search = hash

    function getURLParam(name) {
        return hasher.searchParams.get(name)
    }

    function setURLParams() {
        hasher.searchParams.set('path', current.path);

        let groups = d3.select('#groups_list').property('value');
        hasher.searchParams.set('groups', groups);

        let users = d3.select('#users_list').property('value');
        hasher.searchParams.set('users', users);

        let fts = d3.select('#ft_list').property('value');
        hasher.searchParams.set('fts', fts);

        let area = d3.select('input[name="area"]:checked').property("value");
        hasher.searchParams.set('area', area);

        hasher.searchParams.set('age', age_filter);

        window.location.hash = hasher.searchParams;
    }

    function initialize(root) {
        root.x = root.y = 0;
        root.dx = width;
        root.dy = height;
        root.depth = 0;
    }

    $('.flexdatalist').flexdatalist({
        selectionRequired: 1,
        minLength: 0
    });

    var $filter_inputs = $('.flexdatalist');

    var filterIDs = ['groups_list', 'users_list', 'ft_list'];
    let filters = new Map();

    let age_filter = "0"

    function getFilters() {
        str = "";
        filterIDs.forEach(id => str += id + ':' + filters.get(id) + ';');
        str += "age:" + age_filter;
        return str;
    }

    var updateFilters = function ($target) {
        $target.each(function () {
            filters.set($(this).attr('id'), $(this).val());
        });
    };

    $filter_inputs
        .on('change:flexdatalist', function (e, set) {
            updateFilters($(this));
        });

    function storeFilters(node) {
        currentFilters = getFilters();
        node.filters = currentFilters;

        if (node.children) {
            node.children.forEach(child => child.filters = currentFilters);
        }
    }

    function atimeToDays(node) {
        let atime = new Date(node.atime);
        let now = new Date();
        return Math.round((now - atime) / (1000 * 60 * 60 * 24));
    }

    function atimeToColorClass(node) {
        let days = atimeToDays(node);
        let c = "parent "

        if (days >= 365 * 2) {
            c += "age_2years"
        } else if (days >= 365) {
            c += "age_1year"
        } else if (days >= 304) {
            c += "age_10months"
        } else if (days >= 243) {
            c += "age_8months"
        } else if (days >= 182) {
            c += "age_6months"
        } else if (days >= 91) {
            c += "age_3months"
        } else if (days >= 61) {
            c += "age_2months"
        } else if (days >= 30) {
            c += "age_1month"
        } else {
            c += "age_1week"
        }

        return c
    }

    function atimePassesAgeFilter(node) {
        let days = atimeToDays(node);
        return days >= age_filter
    }

    // Compute the treemap layout recursively such that each group of siblings
    // uses the same size (1×1) rather than the dimensions of the parent cell.
    // This optimizes the layout for the current zoom state. Note that a wrapper
    // object is created for the parent node for each group of siblings so that
    // the parent’s dimensions are not discarded as we recurse. Since each group
    // of sibling was laid out in 1×1, we must rescale to fit using absolute
    // coordinates. This lets us use a viewport to zoom.
    function layout(d) {
        var children = getChildren(d);
        if (children && children.length > 0) {
            treemap.nodes({ children: children });
            children.forEach(function (c) {
                c.x = d.x + c.x * d.dx;
                c.y = d.y + c.y * d.dy;
                c.dx *= d.dx;
                c.dy *= d.dy;
                c.parent = d;
                layout(c);
            });
        }
    }

    function display(d) {
        let gt = grandparent
            .datum(d.parent)
            .on("click", transition)
            .select("text");

        if (d.path == "/") {
            grandparent.style("cursor", "default")
            gt.text('')
        } else {
            grandparent.style("cursor", "pointer")
            gt.text('↵')
        }

        var g1 = svg.insert("g", ".grandparent")
            .datum(d)
            .classed("depth", true);

        var g = g1.selectAll("g")
            .data(getChildren(d))
            .enter().append("g");

        g.filter(function (d) { return d.has_children; })
            .classed("children", true)
            .style("cursor", "pointer")
            .on("click", transition);

        g.filter(function (d) { return !d.has_children; })
            .classed("childless", true)
            .style("cursor", "default");

        g.selectAll(".child")
            .data(function (d) { return getChildren(d) || [d]; })
            .enter().append("rect")
            .classed("child", true)
            .call(rect);

        g.append("rect")
            .attr("class", function (d) { return atimeToColorClass(d) }) // also sets parent class
            .call(rect);

        var titlesvg = g.append("svg")
            .classed("parent_title", true)
            .attr("viewBox", "-100 -10 200 20")
            .attr("preserveAspectRatio", "xMidYMid meet")
            .call(rect);

        titlesvg.append("text")
            .attr("font-size", 16)
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", 200)
            .attr("height", 20)
            .attr("dy", ".3em")
            .style("text-anchor", "middle")
            .text(function (d) { return d.name; });

        g.on("mouseover", mouseover).on("mouseout", mouseout);

        d3.selectAll("#select_area input").on("change", function () {
            areaBasedOnSize = this.value == "size"
            setAllValues()
            transition(current);
        });

        d3.select("#filterButton").on('click', function () {
            age_filter = $("#age_filter").val();

            // console.log('getting fresh filtered data for ', data.path);
            getData(current.path, function (data) {
                cloneProperties(data, current)
                setValues(current);
                storeFilters(current);
                setURLParams();
                transition(current);
            });
        });

        function mouseover(g) {
            showDetails(g)
        }

        function mouseout() {
            showCurrentDetails()
        }

        function transition(d) {
            if (!d && current.path !== "/") {
                parent = current.path.substring(0, current.path.lastIndexOf('/'));
                current.path = parent;
                setURLParams();
                window.location.reload(false);
            }

            if (transitioning || !d) return;
            transitioning = true;

            function do_transition(d) {
                layout(d);

                var g2 = display(d),
                    t1 = g1.transition().duration(250),
                    t2 = g2.transition().duration(250);

                // Update the domain only after entering new elements.
                x.domain([d.x, d.x + d.dx]);
                y.domain([d.y, d.y + d.dy]);

                // Enable anti-aliasing during the transition.
                svg.style("shape-rendering", null);

                // Draw child nodes on top of parent nodes.
                svg.selectAll(".depth").sort(function (a, b) { return a.depth - b.depth; });

                // Fade-in entering text.
                g2.selectAll("text").style("fill-opacity", 0);

                // Transition to the new view.
                t1.selectAll(".parent_title").call(rect);
                t2.selectAll(".parent_title").call(rect);
                t1.selectAll("text").style("fill-opacity", 0);
                t2.selectAll("text").style("fill-opacity", 1);
                t1.selectAll("rect").call(rect);
                t2.selectAll("rect").call(rect);

                // Remove the old node when the transition is finished.
                t1.remove().each("end", function () {
                    svg.style("shape-rendering", "crispEdges");
                    transitioning = false;
                });
            }

            if (d.children && d.filters == getFilters()) {
                // console.log('using old data for ', d.path);
                do_transition(d)
                updateDetails(d);
                setAllFilterOptions(d);
                setURLParams();
                createBreadcrumbs(d.path);
            } else {
                // console.log('getting fresh data for ', d.path);
                getData(d.path, function (data) {
                    cloneProperties(data, d);
                    setValues(d);
                    storeFilters(d);
                    do_transition(d);
                    updateDetails(d);
                    setAllFilterOptions(d);
                    setURLParams();
                });
            }
        }

        return g;
    }

    function rect(rect) {
        rect.attr("x", function (d) { return x(d.x); })
            .attr("y", function (d) { return y(d.y); })
            .attr("width", function (d) { return x(d.x + d.dx) - x(d.x); })
            .attr("height", function (d) { return y(d.y + d.dy) - y(d.y); });
    }

    function getChildren(parent) {
        return parent.children
    }

    function constructAPIURL(path) {
        url = "/rest/v1/auth/tree?path=" + path

        groups = d3.select('#groups_list').property('value');
        users = d3.select('#users_list').property('value');
        filetypes = d3.select('#ft_list').property('value');

        if (groups != "") {
            url += '&groups=' + groups
        }

        if (users != "") {
            url += '&users=' + users
        }

        if (filetypes != "") {
            url += '&types=' + filetypes
        }

        return url
    }

    function getData(path, loadFunction) {
        d3.select("#spinner").style("display", "inline-block")

        fetch(constructAPIURL(path), {
            headers: {
                "Authorization": `Bearer ${cookie.get('jwt')}`
            }
        }).then(r => {
            if (r.status === 401) {
                cookie.remove("jwt", { path: "" });
                window.location.reload();
            } else if (r.status === 400) {
                throw ("bad query")
            } else {
                return r.json()
            }
        }).then(d => {
            d3.select("#spinner").style("display", "none");
            if (d.count > 0) {
                d3.select("#error").text("")
                allNodes.set(d.path, d);
                filterYoungChildren(d);
                loadFunction(d);
                createBreadcrumbs(d.path);
            }
            else {
                d3.select("#error").text("error: no results")
            }
        }).catch(e => {
            d3.select("#spinner").style("display", "none");
            d3.select("#error").text("error: " + e)
        })

    }

    function filterYoungChildren(node) {
        if (!node.children || age_filter < 30) {
            return;
        }

        node.children = node.children.filter(child => atimePassesAgeFilter(child))
    }

    var areaBasedOnSize = true

    function cloneProperties(a, b) {
        b.size = a.size;
        b.count = a.count;
        b.atime = a.atime;
        b.groups = a.groups;
        b.users = a.users;
        b.filetypes = a.filetypes;
        b.children = a.children;
        b.has_children = a.has_children;
    }

    function setValues(d) {
        if (areaBasedOnSize) {
            d.value = d.size;

            if (d.children) {
                d.children.forEach(item => item.value = item.size);
            }
        } else {
            d.value = d.count;

            if (d.children) {
                d.children.forEach(item => item.value = item.count);
            }
        }
    }

    function setAllValues() {
        for (let d of allNodes.values()) {
            setValues(d);
        }
    }

    var BINARY_UNIT_LABELS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];

    function bytesHuman(bytes) {
        var e = Math.floor(Math.log(bytes) / Math.log(1024));
        return parseFloat((bytes / Math.pow(1024, e)).toFixed(2)) + " " + BINARY_UNIT_LABELS[e];
    }

    var NUMBER_UNIT_LABELS = ["", "K", "M", "B", "T", "Q"];

    function countHuman(count) {
        var unit = Math.floor((count / 1.0e+1).toFixed(0).toString().length);
        var r = unit % 3;
        var x = Math.abs(Number(count)) / Number('1.0e+' + (unit - r)).toFixed(2);
        return parseFloat(x.toFixed(2)) + ' ' + NUMBER_UNIT_LABELS[Math.floor(unit / 3)];
    }

    function commaSep(list) {
        return list.join(", ");
    }

    function showDetails(node) {
        d3.select('#details_path').text(node.path);
        d3.select('#details_size').text(bytesHuman(node.size));
        d3.select('#details_count').text(countHuman(node.count));
        d3.select('#details_atime').text(node.atime);
        d3.select('#details_groups').text(commaSep(node.groups));
        d3.select('#details_users').text(commaSep(node.users));
        d3.select('#details_filetypes').text(commaSep(node.filetypes));
    }

    function showCurrentDetails() {
        showDetails(current);
    }

    function updateDetails(node) {
        current = node;
        showCurrentDetails();
    }

    function setAllFilterOptions(data) {
        setFilterOptions('#groups_list', data.groups);
        setFilterOptions('#users_list', data.users);
        setFilterOptions('#ft_list', data.filetypes);
    }

    function setFilterOptions(id, elements) {
        var select = d3.select(id);

        select
            .selectAll('option')
            .remove();

        select
            .selectAll('option')
            .data(elements).enter()
            .append('option')
            .text(function (d) { return d; })
            .property("selected", function (d) { return d === filters.get(id) });
    }

    function setTimestamp(data) {
        $('#timestamp').attr('datetime', data.timestamp)
        $('#timestamp').timeago();
    }

    let path = "/";
    let groups;
    let users;
    let fts;

    function setPageDefaultsFromHash() {
        path = getURLParam('path')
        if (!path || !path.startsWith("/")) {
            path = "/";
        }

        groups = getURLParam('groups');
        if (groups) {
            d3.select('#groups_list').property('value', groups);
        }

        users = getURLParam('users');
        if (users) {
            d3.select('#users_list').property('value', users);
        }

        fts = getURLParam('fts');
        if (fts) {
            d3.select('#ft_list').property('value', fts);
        }

        let area = getURLParam('area');
        if (area) {
            $("#" + area).prop("checked", true);
            areaBasedOnSize = area == "size"
        }

        age_filter = getURLParam('age');
        if (age_filter == null) {
            age_filter = "0";
        }
        $("#age_filter").val(age_filter);

        return path
    }

    setPageDefaultsFromHash();

    function createBreadcrumbs(path) {
        $("#breadcrumbs").empty();

        if (path === "/") {
            $("#breadcrumbs").append('<span></span><button class="dead">/</button>');
            return;
        }

        let dirs = path.split("/");
        dirs[0] = "/";
        let sep = "";
        let max = dirs.length - 1;

        for (var i = 0; i <= max; i++) {
            let dir = dirs[i];

            if (i > 1) {
                sep = "/"
            }

            $("#breadcrumbs").append('<span>' + sep + '</span>');

            if (i < max) {
                let this_id = 'crumb' + i;
                $("#breadcrumbs").append('<button id="' + this_id + '">' + dir + '</button>');

                let this_path = "/" + dirs.slice(1, i + 1).join("/");
                $('#' + this_id).click(function () {
                    current.path = this_path;
                    setURLParams();
                    window.location.reload(false);
                });
            } else {
                $("#breadcrumbs").append('<span>' + dir + '</span>');
            }
        }
    }

    getData(path, function (data) {
        initialize(data);
        setValues(data);
        layout(data);
        display(data);
        updateDetails(data);
        setAllFilterOptions(data);

        if (groups) {
            $('#groups_list').val(groups)
        }

        if (users) {
            $('#users_list').val(users)
        }

        if (fts) {
            $('#ft_list').val(fts)
        }

        storeFilters(data);
        setTimestamp(data);
    });
});