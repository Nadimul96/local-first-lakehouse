(() => {
  const root = document.documentElement;
  const key = "lakehouse-report-theme";
  try {
    const saved = localStorage.getItem(key);
    if (saved === "light" || saved === "dark") root.dataset.theme = saved;
  } catch (_) { /* storage can be unavailable; the OS preference still applies */ }

  const toggle = document.querySelector(".theme-toggle");
  if (toggle) {
    toggle.addEventListener("click", () => {
      const dark = root.dataset.theme
        ? root.dataset.theme === "dark"
        : window.matchMedia("(prefers-color-scheme: dark)").matches;
      root.dataset.theme = dark ? "light" : "dark";
      try { localStorage.setItem(key, root.dataset.theme); } catch (_) { /* ignore */ }
    });
  }

  const tip = document.createElement("div");
  tip.className = "tip";
  tip.setAttribute("role", "tooltip");
  document.body.appendChild(tip);

  const show = (target, x, y) => {
    tip.textContent = target.dataset.tip;
    tip.classList.add("on");
    const box = tip.getBoundingClientRect();
    const left = Math.min(window.innerWidth - box.width - 8, Math.max(8, x - box.width / 2));
    const top = y - box.height - 12 < 8 ? y + 16 : y - box.height - 12;
    tip.style.left = `${left}px`;
    tip.style.top = `${top}px`;
  };
  const hide = () => tip.classList.remove("on");

  document.addEventListener("pointermove", (event) => {
    const target = event.target.closest("[data-tip]");
    if (target) show(target, event.clientX, event.clientY);
    else hide();
  });
  document.addEventListener("focusin", (event) => {
    const target = event.target.closest("[data-tip]");
    if (!target) return;
    const box = target.getBoundingClientRect();
    show(target, box.left + box.width / 2, box.top);
  });
  document.addEventListener("focusout", hide);
})();
