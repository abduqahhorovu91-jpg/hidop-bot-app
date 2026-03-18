const tg = window.Telegram?.WebApp;
const isLocalHost = /^(localhost|127\.0\.0\.1)$/.test(window.location.hostname);
const API_BASE_URL = isLocalHost
  ? `${window.location.protocol}//${window.location.hostname}:8080`
  : (window.location.origin && window.location.origin !== "null"
    ? window.location.origin
    : "http://127.0.0.1:8080");
if (tg) {
  tg.ready();
  tg.expand();
  tg.setHeaderColor("#102318");
  tg.setBackgroundColor("#102318");
}

const playlistEl = document.getElementById("playlist");
const filtersEl = document.getElementById("filters");
const statsEl = document.getElementById("stats");
const libraryListEl = document.getElementById("libraryList");
const libraryCountEl = document.getElementById("libraryCount");
const libraryEmptyEl = document.getElementById("libraryEmpty");
const searchInputEl = document.getElementById("searchInput");
const searchRowEl = document.getElementById("searchRow");
const searchToggleEl = document.getElementById("searchToggle");
const emptyStateEl = document.getElementById("emptyState");
const profileModalEl = document.getElementById("profileModal");
const profileModalBackdropEl = document.getElementById("profileModalBackdrop");
const profileModalCloseEl = document.getElementById("profileModalClose");
const profileButtonEl = document.querySelector(".telegram-bar__profile");
const profileInputEl = document.getElementById("profileInput");
const profileSubmitEl = document.getElementById("profileSubmit");
const topToastEl = document.getElementById("topToast");
const TARGET_USER_STORAGE_KEY = "hidop_target_user_id";

const demoItems = [
  {
    id: 1,
    title: "Merlin 17-QISM",
    comment: "Ombor",
    category: "Ombor",
    duration: 2618,
    ageLabel: "3 oy oldin",
    palette: "instagram",
  },
  {
    id: 2,
    title: "Trend remix",
    comment: "Ombor",
    category: "Ombor",
    duration: 28,
    ageLabel: "1 hafta oldin",
    palette: "instagram",
  },
  {
    id: 3,
    title: "Road vibes",
    comment: "Ombor",
    category: "Ombor",
    duration: 64,
    ageLabel: "2 kun oldin",
    palette: "youtube",
  },
  {
    id: 4,
    title: "Night city loop",
    comment: "Ombor",
    category: "Ombor",
    duration: 42,
    ageLabel: "Kecha",
    palette: "night",
  },
];

const categoryOrder = ["HOME", "Ombor"];
let allItems = [];
let activeCategory = "HOME";
let activeQuery = "";
let catalogItems = [];
let savedItems = [];
let selectedTargetUserId = "";
let toastTimerId = null;

function formatDuration(seconds = 0) {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}:${String(secs).padStart(2, "0")}`;
}

function detectCategory(item) {
  const haystack = `${item.title || ""} ${item.comment || ""}`.toLowerCase();
  if (
    haystack.includes("tiktok") ||
    haystack.includes("instagram") ||
    haystack.includes("youtube") ||
    haystack.includes("youtu") ||
    haystack.includes("ombor")
  ) return "Ombor";
  return "HOME";
}

function detectPalette(item) {
  switch (detectCategory(item)) {
    case "Ombor":
      return "instagram";
    default:
      return "night";
  }
}

async function loadItems() {
  try {
    let items = [];
    try {
      const response = await fetch(`${API_BASE_URL}/api/catalog`, { cache: "no-store" });
      if (!response.ok) throw new Error("catalog topilmadi");
      const payload = await response.json();
      items = Array.isArray(payload?.items) ? payload.items : [];
    } catch {
      const fallbackResponse = await fetch("../videos.json", { cache: "no-store" });
      if (!fallbackResponse.ok) throw new Error("videos.json topilmadi");
      const fallbackPayload = await fallbackResponse.json();
      items = Array.isArray(fallbackPayload?.items) ? fallbackPayload.items : [];
    }

    catalogItems = items.map((item) => ({
      id: Number(item.id || 0),
      title: item.title || "Sarlavha topilmadi",
      comment: item.comment || "",
      category: item.category || detectCategory(item),
      duration: Number(item.duration || 0),
      ageLabel: item.ageLabel || "Kutubxonada",
      palette: item.palette || detectPalette(item),
      preview_url: item.preview_url || "",
    }));
    if (!catalogItems.length) {
      catalogItems = [...demoItems];
      return demoItems;
    }
    return catalogItems.map((item) => ({
      id: item.id,
      title: item.title || "Sarlavha topilmadi",
      comment: item.comment || item.category || detectCategory(item),
      category: item.category || detectCategory(item),
      duration: Number(item.duration || 0),
      ageLabel: "Kutubxonada",
      palette: item.palette || detectPalette(item),
      preview_url: item.preview_url || "",
    }));
  } catch {
    catalogItems = [...demoItems];
    return demoItems;
  }
}

async function loadSavedItems() {
  const ownerId = getActiveOwnerId();
  if (!ownerId) {
    return [];
  }

  try {
    try {
      const response = await fetch(
        `${API_BASE_URL}/api/saved-videos?owner_id=${encodeURIComponent(ownerId)}`,
        { cache: "no-store" }
      );
      if (!response.ok) throw new Error("saved videos topilmadi");
      const payload = await response.json();
      if (Array.isArray(payload?.items)) {
        return payload.items;
      }
    } catch {
      // Fallback to local files below.
    }

    const fallbackResponse = await fetch("../saved_videos.json", { cache: "no-store" });
    if (!fallbackResponse.ok) throw new Error("saved_videos.json topilmadi");
    const fallbackPayload = await fallbackResponse.json();
    if (!fallbackPayload || typeof fallbackPayload !== "object") return [];

    const rawSavedItems = Array.isArray(fallbackPayload[ownerId])
      ? fallbackPayload[ownerId]
      : [];
    const catalogById = new Map(catalogItems.map((item) => [Number(item.id), item]));

    return rawSavedItems
      .map((savedItem) => {
        const videoId = Number(savedItem.video_id ?? savedItem.saved_id);
        const catalogItem = catalogById.get(videoId);
        if (!catalogItem) return null;
        return {
          ...catalogItem,
          saved_name: String(savedItem.name || catalogItem.title || ""),
          category: "Ombor",
          ageLabel: "Saqlangan",
          palette: "instagram",
        };
      })
      .filter(Boolean);
  } catch {
    return [];
  }
}

function getActiveOwnerId() {
  return selectedTargetUserId || "";
}

async function refreshSavedItems() {
  savedItems = await loadSavedItems();
}

function buildStats(items) {
  if (!statsEl) return;

  const total = items.length;
  const totalDuration = items.reduce((sum, item) => sum + Number(item.duration || 0), 0);
  const categories = new Set(items.map((item) => item.category).filter(Boolean));

  statsEl.innerHTML = "";
  [
    `${total} ta video`,
    `${categories.size} ta bo'lim`,
    `${Math.max(1, Math.round(totalDuration / 60))} daqiqa kontent`,
  ].forEach((label) => {
    const pill = document.createElement("div");
    pill.className = "stat-pill";
    pill.textContent = label;
    statsEl.appendChild(pill);
  });
}

function getPlatformColor(category) {
  if (category === "Ombor") return "#ff5353";
  return "#f4f0dc";
}

function sendVideoToBot(item) {
  if (!item) return;
  if (!selectedTargetUserId) {
    openProfileModal();
    profileInputEl?.focus();
    return;
  }
  const payload = {
    type: "send_video",
    video_id: item.id,
    title: item.saved_name || item.title || "",
    source: activeCategory,
    target_user_id: selectedTargetUserId,
  };

  if (tg) {
    try {
      fetch(`${API_BASE_URL}/api/send-video`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
        .then((response) => response.json())
        .then((result) => {
          if (result?.ok) {
            showTopToast("yuborildi ✅");
            return;
          }
          window.alert(result?.message || result?.error || "Video yuborilmadi.");
        })
        .catch(() => {
          window.alert("Video yuborishda xatolik bo'ldi.");
        });
      showTopToast("yuborilmoqda ✅");
      return;
    } catch (_) {
      window.alert("Video yuborishda xatolik bo'ldi.");
      return;
    }
  }

  fetch(`${API_BASE_URL}/api/send-video`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  })
    .then((response) => response.json())
    .then((result) => {
      if (result?.ok) {
        showTopToast("yuborildi ✅");
        return;
      }
      window.alert(result?.message || result?.error || "Video yuborilmadi.");
    })
    .catch(() => {
      window.alert("Video yuborishda xatolik bo'ldi.");
    });
}

function deleteSavedVideo(item) {
  if (!item?.id) return;
  const ownerId = getActiveOwnerId();
  if (!ownerId) {
    openProfileModal();
    profileInputEl?.focus();
    return;
  }
  fetch(`${API_BASE_URL}/api/delete-saved-video`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      owner_id: ownerId,
      video_id: item.id,
    }),
  })
    .then((response) => response.json())
    .then((result) => {
      if (!result?.ok) {
        window.alert(result?.message || result?.error || "Video o'chirilmadi.");
        return;
      }
      savedItems = savedItems.filter((savedItem) => Number(savedItem.id) !== Number(item.id));
      render();
      renderLibrary();
      showTopToast("ombordan olib tashlandi ✅");
    })
    .catch(() => {
      window.alert("Video o'chirishda xatolik bo'ldi.");
    });
}

function matchesSearch(item) {
  if (!activeQuery) return true;
  const haystack = [
    item.title || "",
    item.comment || "",
    item.category || "",
    String(item.id ?? ""),
  ].join(" ").toLowerCase();
  return haystack.includes(activeQuery);
}

function getSearchScore(item) {
  if (!activeQuery) return 0;

  const title = String(item.title || "").toLowerCase().trim();
  const comment = String(item.comment || "").toLowerCase().trim();
  const category = String(item.category || "").toLowerCase().trim();
  const idText = String(item.id ?? "").toLowerCase().trim();

  if (title === activeQuery) return 1000;
  if (idText === activeQuery) return 950;
  if (title.startsWith(activeQuery)) return 900 - Math.min(title.length, 200);

  const titleWords = title.split(/\s+/).filter(Boolean);
  if (titleWords.some((word) => word.startsWith(activeQuery))) return 820;

  if (comment.startsWith(activeQuery) || category.startsWith(activeQuery)) return 760;
  if (title.includes(activeQuery)) return 680;
  if (comment.includes(activeQuery) || category.includes(activeQuery)) return 560;
  if (idText.includes(activeQuery)) return 520;

  return 0;
}

function normalizeSearchText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractEpisodeNumber(value) {
  const text = normalizeSearchText(value);
  const episodePatterns = [
    /(\d+)\s*-\s*qism\b/,
    /(\d+)\s*qism\b/,
    /\bqism\s*(\d+)\b/,
    /\bpart\s*(\d+)\b/,
    /\bep(?:isode)?\s*(\d+)\b/,
  ];

  for (const pattern of episodePatterns) {
    const match = text.match(pattern);
    if (match) {
      return Number(match[1]);
    }
  }

  const trailingNumber = text.match(/(\d+)(?!.*\d)/);
  return trailingNumber ? Number(trailingNumber[1]) : Number.POSITIVE_INFINITY;
}

function getTitleBase(value) {
  return normalizeSearchText(value)
    .replace(/\b\d+\s*-\s*qism\b/g, "")
    .replace(/\b\d+\s*qism\b/g, "")
    .replace(/\bqism\s*\d+\b/g, "")
    .replace(/\bpart\s*\d+\b/g, "")
    .replace(/\bep(?:isode)?\s*\d+\b/g, "")
    .replace(/\b\d+\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function sortBySearchRelevance(items) {
  const naturalCollator = new Intl.Collator("uz", {
    numeric: true,
    sensitivity: "base",
  });

  return items.slice().sort((left, right) => {
    const scoreDiff = getSearchScore(right) - getSearchScore(left);
    if (scoreDiff !== 0) return scoreDiff;

    const leftTitle = normalizeSearchText(left.saved_name || left.title || "");
    const rightTitle = normalizeSearchText(right.saved_name || right.title || "");
    const leftBase = getTitleBase(leftTitle);
    const rightBase = getTitleBase(rightTitle);

    if (activeQuery) {
      const leftBaseStarts = leftBase.startsWith(activeQuery);
      const rightBaseStarts = rightBase.startsWith(activeQuery);
      if (leftBaseStarts !== rightBaseStarts) return rightBaseStarts - leftBaseStarts;
    }

    const baseDiff = leftBase.localeCompare(rightBase);
    if (baseDiff !== 0) return baseDiff;

    const leftEpisode = extractEpisodeNumber(leftTitle);
    const rightEpisode = extractEpisodeNumber(rightTitle);
    const leftEpisodeRank = Number.isFinite(leftEpisode) ? leftEpisode : Number.MAX_SAFE_INTEGER;
    const rightEpisodeRank = Number.isFinite(rightEpisode) ? rightEpisode : Number.MAX_SAFE_INTEGER;
    const episodeDiff = leftEpisodeRank - rightEpisodeRank;
    if (episodeDiff !== 0) return episodeDiff;

    const titleDiff = naturalCollator.compare(leftTitle, rightTitle);
    if (titleDiff !== 0) return titleDiff;

    return Number(left.id || 0) - Number(right.id || 0);
  });
}

function renderLibrary() {
  const sourceItems = activeCategory === "Ombor" ? savedItems : catalogItems;
  const filteredCatalog = sourceItems.filter((item) => {
    const categoryOk = activeCategory === "HOME" || item.category === activeCategory;
    return categoryOk && matchesSearch(item);
  });
  const orderedCatalog = sortBySearchRelevance(filteredCatalog);

  libraryListEl.innerHTML = "";
  libraryCountEl.textContent = `${orderedCatalog.length} ta`;
  libraryEmptyEl.classList.toggle("is-hidden", orderedCatalog.length > 0);

  orderedCatalog
    .forEach((item) => {
      const row = document.createElement("article");
      row.className = "library-item";
      row.innerHTML = `
        <div class="library-item__id">#${item.id ?? "?"}</div>
        <div>
          <h3 class="library-item__title">${item.saved_name || item.title || "Sarlavha topilmadi"}</h3>
        </div>
        <div class="library-item__actions">
          <button class="send-button" type="button">Yuborish</button>
          ${activeCategory === "Ombor" ? '<button class="more-button" type="button">…</button>' : ""}
        </div>
        <div class="library-item__duration">${formatDuration(item.duration)}</div>
      `;
      row.querySelector(".send-button")?.addEventListener("click", (event) => {
        event.stopPropagation();
        sendVideoToBot(item);
      });
      row.querySelector(".more-button")?.addEventListener("click", (event) => {
        event.stopPropagation();
        deleteSavedVideo(item);
      });
      row.addEventListener("click", () => {
        if (tg) {
          tg.sendData(JSON.stringify({ type: "catalog_item", video_id: item.id, title: item.title }));
        }
      });
      libraryListEl.appendChild(row);
    });
}

function buildFilters(items) {
  const discovered = new Set(["HOME", "Ombor"]);
  items.forEach((item) => {
    if (item.category && !discovered.has(item.category)) {
      discovered.add(item.category);
    }
  });

  filtersEl.innerHTML = "";
  [...discovered].forEach((category) => {
    const button = document.createElement("button");
    button.className = `filter-chip${category === activeCategory ? " is-active" : ""}`;
    button.textContent = category;
    button.type = "button";
    button.addEventListener("click", () => {
      if (category === "Ombor" && !selectedTargetUserId) {
        activeCategory = "Ombor";
        buildFilters(items);
        render();
        renderLibrary();
        openProfileModal();
        return;
      }
      activeCategory = category;
      buildFilters(items);
      render();
      renderLibrary();
    });
    filtersEl.appendChild(button);
  });
}

function render() {
  const sourceItems = activeCategory === "Ombor" ? savedItems : allItems;
  const filtered = sourceItems.filter((item) => {
    const categoryOk = activeCategory === "HOME" || item.category === activeCategory;
    return categoryOk && matchesSearch(item);
  });
  const ordered = sortBySearchRelevance(filtered);

  playlistEl.innerHTML = "";
  emptyStateEl.classList.toggle("is-hidden", ordered.length > 0);

  ordered.forEach((item, index) => {
    const card = document.createElement("article");
    card.className = "card";
    card.style.animationDelay = `${Math.min(260, index * 60)}ms`;
    const platformColor = getPlatformColor(item.category);
    card.innerHTML = `
      <div class="thumb thumb--${item.palette || "night"}${item.preview_url ? " has-video" : ""}">
        ${item.preview_url ? `<video src="${item.preview_url}" autoplay muted loop playsinline preload="metadata"></video>` : ""}
        <div class="thumb__label">${item.saved_name || item.title || "Sarlavha topilmadi"}</div>
        <div class="thumb__badge">${formatDuration(item.duration)}</div>
        <div class="thumb__platform" style="color:${platformColor}">
          <span class="thumb__platform-dot"></span>
          ${item.category || "Media"}
        </div>
      </div>
      <div class="meta">
        <div class="avatar" aria-hidden="true">·</div>
        <div class="meta__content">
          <div class="meta__top">
            <h3>${item.saved_name || item.title || "Sarlavha topilmadi"}</h3>
            <div class="meta__actions">
              <button class="send-button" type="button">Yuborish</button>
              ${activeCategory === "Ombor" ? '<button class="delete-button" type="button">O\'chirish</button>' : ""}
            </div>
          </div>
          <p>${item.comment || item.category} • ${item.ageLabel}</p>
        </div>
      </div>
    `;

    card.querySelector(".send-button")?.addEventListener("click", (event) => {
      event.stopPropagation();
      sendVideoToBot(item);
    });
    card.querySelector(".delete-button")?.addEventListener("click", (event) => {
      event.stopPropagation();
      deleteSavedVideo(item);
    });
    card.addEventListener("click", () => {
      if (tg) {
        tg.sendData(JSON.stringify({ type: "playlist_item", video_id: item.id, title: item.title }));
      }
    });

    playlistEl.appendChild(card);
  });
}

searchToggleEl.addEventListener("click", () => {
  searchRowEl.classList.toggle("is-hidden");
  if (!searchRowEl.classList.contains("is-hidden")) {
    searchInputEl.focus();
  } else {
    searchInputEl.value = "";
    activeQuery = "";
    render();
    renderLibrary();
  }
});

searchInputEl.addEventListener("input", (event) => {
  activeQuery = event.target.value.trim().toLowerCase();
  render();
  renderLibrary();
});

function openProfileModal() {
  syncProfileUi();
  profileModalEl?.classList.remove("is-hidden");
  profileInputEl?.focus();
}

function closeProfileModal() {
  profileModalEl?.classList.add("is-hidden");
}

function showTopToast(message) {
  if (!topToastEl) return;
  topToastEl.textContent = message;
  topToastEl.classList.remove("is-hidden");
  if (toastTimerId) {
    window.clearTimeout(toastTimerId);
  }
  toastTimerId = window.setTimeout(() => {
    topToastEl.classList.add("is-hidden");
  }, 2200);
}

function syncProfileUi() {
  if (!profileInputEl || !profileSubmitEl) return;
  profileInputEl.value = selectedTargetUserId;
  profileInputEl.readOnly = Boolean(selectedTargetUserId);
  profileInputEl.placeholder = selectedTargetUserId ? "ID saqlangan" : "ID ingizni kiriting";
  profileSubmitEl.textContent = selectedTargetUserId ? "O'CHIRISH" : "KIRISH";
  profileSubmitEl.classList.toggle("profile-card__submit--danger", Boolean(selectedTargetUserId));
}

function loadStoredTargetUserId() {
  try {
    const savedValue = window.localStorage.getItem(TARGET_USER_STORAGE_KEY) || "";
    selectedTargetUserId = /^\d+$/.test(savedValue) ? savedValue : "";
  } catch {
    selectedTargetUserId = "";
  }
  syncProfileUi();
}

function persistTargetUserId(value) {
  try {
    if (value) {
      window.localStorage.setItem(TARGET_USER_STORAGE_KEY, value);
    } else {
      window.localStorage.removeItem(TARGET_USER_STORAGE_KEY);
    }
  } catch {
    // Ignore storage issues.
  }
}

async function submitProfileId() {
  if (selectedTargetUserId) {
    const wasInOmbor = activeCategory === "Ombor";
    selectedTargetUserId = "";
    persistTargetUserId("");
    savedItems = [];
    if (wasInOmbor) {
      activeCategory = "HOME";
    }
    syncProfileUi();
    buildFilters(allItems);
    render();
    renderLibrary();
    showTopToast("o'chirildi ✅");
    return;
  }

  const rawValue = String(profileInputEl?.value || "").trim();
  if (!/^\d+$/.test(rawValue)) {
    window.alert("ID raqam bo'lishi kerak.");
    profileInputEl?.focus();
    return;
  }

  selectedTargetUserId = rawValue;
  persistTargetUserId(rawValue);
  syncProfileUi();
  await refreshSavedItems();
  activeCategory = "Ombor";
  buildFilters(allItems);
  render();
  renderLibrary();
  closeProfileModal();
  showTopToast("saqlandi ✅");
}

profileButtonEl?.addEventListener("click", openProfileModal);
profileModalBackdropEl?.addEventListener("click", closeProfileModal);
profileModalCloseEl?.addEventListener("click", closeProfileModal);
profileSubmitEl?.addEventListener("click", submitProfileId);
profileInputEl?.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    submitProfileId();
  }
});

loadStoredTargetUserId();

allItems = await loadItems();
await refreshSavedItems();
buildStats(allItems);
buildFilters(allItems);
render();
renderLibrary();
