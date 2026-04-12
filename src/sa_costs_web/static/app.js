(function () {
  var deferredInstallPrompt = null;
  var nav = document.querySelector("[data-nav]");
  var navToggle = document.querySelector("[data-nav-toggle]");
  var navBackdrop = document.querySelector("[data-nav-backdrop]");
  var installButton = document.querySelector("[data-install-button]");
  var compactNavQuery = window.matchMedia("(max-width: 1080px)");

  function isCompactNav() {
    return compactNavQuery.matches;
  }

  function setNavOpen(isOpen) {
    if (!nav || !navToggle) {
      return;
    }
    var shouldOpen = isCompactNav() && isOpen;
    nav.classList.toggle("is-open", shouldOpen);
    navToggle.classList.toggle("is-active", shouldOpen);
    navToggle.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
    document.body.classList.remove("nav-open");
    if (navBackdrop) {
      navBackdrop.hidden = true;
    }
  }

  function syncNavState() {
    if (!nav || !navToggle) {
      return;
    }
    if (!isCompactNav()) {
      setNavOpen(false);
    }
  }

  if (navToggle) {
    navToggle.addEventListener("click", function () {
      if (!isCompactNav()) {
        return;
      }
      setNavOpen(!nav.classList.contains("is-open"));
    });
  }

  if (navBackdrop) {
    navBackdrop.addEventListener("click", function () {
      setNavOpen(false);
    });
  }

  document.addEventListener("click", function (event) {
    var chartButton = event.target.closest("[data-chart-mode-button]");
    if (chartButton) {
      var collection = chartButton.closest("[data-chart-collection]");
      if (!collection) {
        return;
      }

      var mode = chartButton.getAttribute("data-chart-mode-button");
      collection.querySelectorAll("[data-chart-mode-button]").forEach(function (item) {
        var isActive = item === chartButton;
        item.classList.toggle("is-active", isActive);
        item.setAttribute("aria-pressed", isActive ? "true" : "false");
      });

      collection.querySelectorAll("[data-chart-card]").forEach(function (card) {
        card.querySelectorAll("[data-chart-view]").forEach(function (view) {
          view.hidden = view.getAttribute("data-chart-view") !== mode;
        });
      });
      return;
    }

    if (isCompactNav() && event.target.closest(".topnav a")) {
      setNavOpen(false);
      return;
    }

    if (isCompactNav() && nav && nav.classList.contains("is-open") && !event.target.closest(".topbar")) {
      setNavOpen(false);
    }
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      setNavOpen(false);
    }
  });

  window.addEventListener("resize", syncNavState);
  syncNavState();

  window.addEventListener("beforeinstallprompt", function (event) {
    event.preventDefault();
    deferredInstallPrompt = event;
    if (installButton) {
      installButton.hidden = false;
    }
  });

  window.addEventListener("appinstalled", function () {
    deferredInstallPrompt = null;
    if (installButton) {
      installButton.hidden = true;
    }
  });

  if (installButton) {
    installButton.addEventListener("click", async function () {
      if (!deferredInstallPrompt) {
        return;
      }
      deferredInstallPrompt.prompt();
      await deferredInstallPrompt.userChoice;
      deferredInstallPrompt = null;
      installButton.hidden = true;
      setNavOpen(false);
    });
  }

  if ("serviceWorker" in navigator) {
    window.addEventListener("load", function () {
      navigator.serviceWorker.register("/sw.js").catch(function () {
        return null;
      });
    });
  }
})();
