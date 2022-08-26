require.config({
    paths: {
        d3: "d3.v3.min",
        queue: "queue.v1",
        lodash: "lodash.min",
        jquery: "jquery-3.6.0.min",
        cookie: "js.cookie.min",
        flexdatalist: "jquery.flexdatalist.min",
        timeago: "jquery.timeago"
    }
});

requirejs.config({
    shim: {
        'flexdatalist': {
            deps: ['jquery']
        },
        'timeago': {
            deps: ['jquery']
        },
    }
});

requirejs(['jquery', 'cookie', 'flexdatalist', 'timeago'], function ($, cookie) {
    function showMap(jwt) {
        let user = "";

        try {
            user = getUsernameFromJWT(jwt);
        }
        catch {
            logout();
        }

        if (!user) {
            logout();
        }

        $("#login").hide()
        $("#body").show()
        $("#username").text(user);

        require(["dtreemap"], function () { });
    }

    $(document).ready(function () {
        jwt = cookie.get('jwt')

        if (jwt && jwt.length !== 0) {
            showMap(jwt)
            return
        }

        let oidc = cookie.get("okta-hosted-login-session-store");
        if (oidc && oidc.length !== 0) {
            fetch("/rest/v1/jwt", {
                method: "POST"
            })
                .then(d => d.json())
                .then((token) => {
                    cookie.set("jwt", token, { path: "", secure: true, sameSite: "strict" });
                    showMap(token);
                })
        } else {
            $("#login").show()
        }
    });

    $("#loginForm").submit(function (event) {
        event.preventDefault();

        var posting = $.post("/rest/v1/jwt", $("#loginForm").serialize(), "json");

        posting.done(function (data) {
            cookie.set('jwt', data, { path: '', secure: true, sameSite: 'strict' })

            showMap(data)
        });

        posting.fail(function () {
            $("#loginFailure").empty().append("Incorrect username or password");
            $("#body").hide()
        });
    });

    function getUsernameFromJWT(token) {
        if (!token) {
            logout()
        }

        return JSON.parse(atob(token.split('.')[1])).Username;
    }

    function logout() {
        cookie.remove("jwt", { path: "" });
        cookie.remove("okta-hosted-login-session-store");
        window.location.reload();
    }

    $("#resetButton").click(() => {
        window.location.hash = '';
        window.location.reload(false);
    });

    $("#logoutButton").click(() => {
        logout()
    });
});
