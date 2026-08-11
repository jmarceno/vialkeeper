(function () {
  "use strict";

  var TOKEN_KEY = "elixir_db_bearer";
  var START_EVENT = "elixirdb:start";
  var LOGOUT_EVENT = "elixirdb:logout";

  function token() {
    try {
      return sessionStorage.getItem(TOKEN_KEY) || "";
    } catch (_error) {
      return "";
    }
  }

  function setToken(value) {
    try {
      if (value) {
        sessionStorage.setItem(TOKEN_KEY, value);
      } else {
        sessionStorage.removeItem(TOKEN_KEY);
      }
    } catch (_error) {
      /* sessionStorage may be unavailable; auth simply fails closed. */
    }
  }

  function appRoot() {
    return document.getElementById("app");
  }

  function authForm() {
    return document.getElementById("auth-form");
  }

  function showAuthForm() {
    var form = authForm();
    var app = appRoot();
    if (app) {
      app.innerHTML = "";
    }
    if (form) {
      form.classList.add("is-visible");
      var input = form.querySelector('input[name="bearer_token"]');
      if (input) {
        input.value = "";
        input.focus();
      }
    }
  }

  function hideAuthForm() {
    var form = authForm();
    if (form) {
      form.classList.remove("is-visible");
    }
  }

  function dispatchStart() {
    document.body.dispatchEvent(new Event(START_EVENT, { bubbles: true }));
  }

  function onReady() {
    document.body.addEventListener(LOGOUT_EVENT, function () {
      setToken("");
      showAuthForm();
    });

    var form = authForm();
    if (form) {
      form.addEventListener("submit", function (event) {
        event.preventDefault();
        var input = form.querySelector('input[name="bearer_token"]');
        var value = input ? String(input.value || "") : "";
        setToken(value);
        if (input) {
          input.value = "";
        }
        hideAuthForm();
        dispatchStart();
      });
    }

    document.body.addEventListener("click", function (event) {
      var target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      var button = target.closest("[data-elixirdb-logout]");
      if (!button) {
        return;
      }
      event.preventDefault();
      document.body.dispatchEvent(new Event(LOGOUT_EVENT, { bubbles: true }));
    });

    hideAuthForm();

    // Wait for window load so deferred HTMX has processed hx-trigger listeners
    // before the first elixirdb:start event is dispatched.
    if (document.readyState === "complete") {
      dispatchStart();
    } else {
      window.addEventListener("load", dispatchStart, { once: true });
    }
  }

  document.addEventListener("htmx:configRequest", function (event) {
    var value = token();
    if (!value) {
      return;
    }
    event.detail.headers["Authorization"] = "Bearer " + value;
  });

  document.addEventListener("htmx:responseError", function (event) {
    var xhr = event.detail && event.detail.xhr;
    if (!xhr || xhr.status !== 401) {
      return;
    }
    setToken("");
    showAuthForm();
  });

  document.addEventListener("htmx:afterRequest", function (event) {
    var xhr = event.detail && event.detail.xhr;
    if (!xhr) {
      return;
    }
    if (xhr.status === 401) {
      setToken("");
      showAuthForm();
      return;
    }
    // When home succeeds without an Authorization header, drop any leftover
    // session token. Do not clear after authenticated successes.
    if (xhr.status >= 200 && xhr.status < 300) {
      var path = "";
      try {
        path = new URL(xhr.responseURL || "", window.location.origin).pathname;
      } catch (_error) {
        path = "";
      }
      if (path !== "/ui/fragments/home") {
        return;
      }
      var headers =
        (event.detail.requestConfig && event.detail.requestConfig.headers) || {};
      var authorization = headers.Authorization || headers.authorization || "";
      if (!authorization) {
        setToken("");
      }
    }
  });

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", onReady);
  } else {
    onReady();
  }
})();
