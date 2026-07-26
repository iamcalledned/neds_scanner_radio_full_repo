(function () {
    'use strict';

    const STORAGE_KEY = 'scanner_global_player_v1';
    const SPEEDS = [0.75, 1, 1.25, 1.5, 2];
    const PLAYER_SELECTOR = [
        '.home-wave-player[data-src]',
        '.town-card-player[data-src]',
        '.wave-player[data-src]',
        '[data-audio]'
    ].join(',');

    let audio;
    let dock;
    let titleEl;
    let subtitleEl;
    let badgeEl;
    let toggleBtn;
    let playIcon;
    let pauseIcon;
    let progressEl;
    let currentEl;
    let durationEl;
    let speedBtn;
    let closeBtn;
    let metadata = {};
    let lastSavedSecond = -1;

    function normalizeSource(source) {
        if (!source) return '';
        try {
            return new URL(source, window.location.href).href;
        } catch (_) {
            return source;
        }
    }

    function formatTime(seconds) {
        if (!Number.isFinite(seconds) || seconds < 0) return '0:00';
        const minutes = Math.floor(seconds / 60);
        const remainder = Math.floor(seconds % 60);
        return `${minutes}:${String(remainder).padStart(2, '0')}`;
    }

    function formatFeed(feed) {
        if (!feed) return 'Scanner call';
        const normalized = String(feed).toLowerCase();
        const type = normalized.includes('fd') ? 'Fire' : 'Police';
        return `${String(feed).toUpperCase()} ${type}`;
    }

    function isCurrentSource(source) {
        const requested = normalizeSource(source);
        const current = normalizeSource(audio?.currentSrc || audio?.src);
        return Boolean(requested && current && requested === current);
    }

    function setDockVisible(visible) {
        if (!dock) return;
        dock.classList.toggle('hidden', !visible);
        document.body.classList.toggle('scanner-player-open', visible);
    }

    function setMetadata(nextMetadata) {
        metadata = {
            title: nextMetadata?.title || 'Scanner audio',
            subtitle: nextMetadata?.subtitle || 'Ned’s Scanner Network',
            badge: nextMetadata?.badge || 'Scanner',
            feed: nextMetadata?.feed || '',
            transcript: nextMetadata?.transcript || ''
        };

        titleEl.textContent = metadata.title;
        subtitleEl.textContent = metadata.subtitle;
        badgeEl.textContent = metadata.badge;

        if ('mediaSession' in navigator && 'MediaMetadata' in window) {
            navigator.mediaSession.metadata = new MediaMetadata({
                title: metadata.title,
                artist: metadata.subtitle,
                album: 'Ned’s Scanner Network',
                artwork: [
                    { src: '/scanner/static/icons/icon-192x192-v2.png', sizes: '192x192', type: 'image/png' },
                    { src: '/scanner/static/icons/icon-512x512-v2.png', sizes: '512x512', type: 'image/png' }
                ]
            });
        }
    }

    function getState() {
        return {
            src: normalizeSource(audio?.currentSrc || audio?.src),
            currentTime: Number.isFinite(audio?.currentTime) ? audio.currentTime : 0,
            duration: Number.isFinite(audio?.duration) ? audio.duration : 0,
            paused: audio ? audio.paused : true,
            playbackRate: audio?.playbackRate || 1,
            metadata: { ...metadata }
        };
    }

    function emitState() {
        const state = getState();
        document.dispatchEvent(new CustomEvent('scannerplayerstate', { detail: state }));
        syncInlinePlayers(state);
    }

    function updateMediaPosition() {
        if (!('mediaSession' in navigator) || typeof navigator.mediaSession.setPositionState !== 'function') return;
        if (!Number.isFinite(audio.duration) || audio.duration <= 0) return;
        try {
            navigator.mediaSession.setPositionState({
                duration: audio.duration,
                playbackRate: audio.playbackRate,
                position: Math.min(audio.currentTime, audio.duration)
            });
        } catch (_) {
            // Some browsers reject position updates while metadata is still loading.
        }
    }

    function updateUi() {
        if (!audio) return;
        const duration = Number.isFinite(audio.duration) ? audio.duration : 0;
        const currentTime = Number.isFinite(audio.currentTime) ? audio.currentTime : 0;
        const playing = !audio.paused && !audio.ended;

        playIcon.classList.toggle('hidden', playing);
        pauseIcon.classList.toggle('hidden', !playing);
        toggleBtn.setAttribute('aria-label', playing ? 'Pause scanner audio' : 'Play scanner audio');
        currentEl.textContent = formatTime(currentTime);
        durationEl.textContent = formatTime(duration);
        progressEl.value = duration > 0 ? String(Math.round((currentTime / duration) * 1000)) : '0';
        speedBtn.textContent = `${audio.playbackRate}×`;

        if ('mediaSession' in navigator) {
            navigator.mediaSession.playbackState = playing ? 'playing' : 'paused';
        }
        updateMediaPosition();
        emitState();
    }

    function persistState() {
        if (!audio?.src) return;
        const state = {
            ...getState(),
            wasPlaying: !audio.paused && !audio.ended,
            updatedAt: Date.now()
        };
        try {
            sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
        } catch (_) {
            // Playback remains functional when storage is unavailable.
        }
    }

    function clearPersistedState() {
        try {
            sessionStorage.removeItem(STORAGE_KEY);
        } catch (_) {
            // Ignore storage restrictions.
        }
    }

    async function playItem(item, options = {}) {
        const source = normalizeSource(item?.src);
        if (!source) return;

        const sameSource = isCurrentSource(source);
        if (sameSource && options.toggle !== false) {
            if (audio.paused) {
                try {
                    await audio.play();
                } catch (error) {
                    console.warn('[Player] Playback could not start:', error);
                }
            } else {
                audio.pause();
            }
            return;
        }

        if (!sameSource) {
            audio.src = source;
            setMetadata(item);
            audio.load();
        } else if (item?.title) {
            setMetadata(item);
        }

        setDockVisible(true);

        if (Number.isFinite(options.startRatio)) {
            audio.addEventListener('loadedmetadata', () => {
                audio.currentTime = Math.max(0, Math.min(1, options.startRatio)) * (audio.duration || 0);
                updateUi();
            }, { once: true });
        }

        if (options.autoplay === false) {
            updateUi();
            persistState();
            return;
        }

        try {
            await audio.play();
        } catch (error) {
            console.warn('[Player] Playback could not start:', error);
            updateUi();
        }
    }

    function seekRatio(ratio) {
        if (!Number.isFinite(audio.duration) || audio.duration <= 0) return;
        audio.currentTime = Math.max(0, Math.min(1, ratio)) * audio.duration;
        updateUi();
        persistState();
    }

    function stopAndClose() {
        audio.pause();
        audio.removeAttribute('src');
        audio.load();
        metadata = {};
        setDockVisible(false);
        clearPersistedState();
        updateUi();
    }

    function playerItemFromElement(player) {
        const card = player.closest(
            '.call-card-entry, .archive-call-card, .call-entry, .review-call-card, .feed-card, article'
        );
        const feed = player.dataset.feed || card?.dataset.feed || '';
        const isFire = String(feed).toLowerCase().includes('fd');
        const title =
            player.dataset.title ||
            card?.querySelector('.call-card-title, h3, h2')?.textContent?.trim() ||
            formatFeed(feed);
        const pill = card?.querySelector('.call-card-pill')?.textContent?.trim() || '';
        const time = card?.querySelector('.call-card-time, [data-call-time]')?.textContent?.trim() || '';
        const transcript =
            player.dataset.transcript ||
            card?.querySelector('.call-card-transcript, .transcript-block')?.textContent?.trim() ||
            '';
        const subtitle =
            player.dataset.subtitle ||
            [pill, time].filter(Boolean).join(' · ') ||
            formatFeed(feed);

        return {
            src: player.dataset.src || player.dataset.audio || '',
            title,
            subtitle,
            transcript,
            feed,
            badge: isFire ? 'Fire' : (feed ? 'Police' : 'Scanner')
        };
    }

    function syncInlinePlayers(state) {
        document.querySelectorAll(PLAYER_SELECTOR).forEach((player) => {
            const source = player.dataset.src || player.dataset.audio || '';
            const active = normalizeSource(source) === state.src;
            const playing = active && !state.paused;
            const button = player.querySelector('.home-wave-btn, .town-card-btn, .wave-play-btn');
            const icon = player.querySelector('.home-wave-icon, .town-card-icon, .wave-play-icon');
            const time = player.querySelector('.home-wave-time, .town-card-time, .wave-time');

            if (icon) icon.textContent = playing ? '⏸' : '▶';
            if (button) {
                button.setAttribute('aria-label', playing ? 'Pause scanner call' : 'Play scanner call');
                button.setAttribute('aria-pressed', playing ? 'true' : 'false');
            }
            if (active && time) time.textContent = formatTime(state.currentTime);
        });
    }

    function handleExistingPlayerClick(event) {
        const playButton = event.target.closest('.home-wave-btn, .town-card-btn, .wave-play-btn');
        const scrub = event.target.closest('.home-wave-scrub, .town-card-scrub, .wave-scrub');
        const simpleWave = event.target.closest('[data-audio]');
        const player = (playButton || scrub)?.closest(PLAYER_SELECTOR) || simpleWave;
        if (!player) return;

        event.preventDefault();
        event.stopImmediatePropagation();

        const item = playerItemFromElement(player);
        if (scrub || simpleWave) {
            const surface = scrub || simpleWave;
            const rect = surface.getBoundingClientRect();
            const ratio = rect.width > 0 ? (event.clientX - rect.left) / rect.width : 0;
            if (isCurrentSource(item.src)) {
                seekRatio(ratio);
                if (audio.paused) audio.play().catch(() => {});
            } else {
                playItem(item, { toggle: false, startRatio: ratio });
            }
            return;
        }

        playItem(item);
    }

    function registerMediaSessionActions() {
        if (!('mediaSession' in navigator)) return;

        const actions = {
            play: () => audio.play(),
            pause: () => audio.pause(),
            seekbackward: (details) => {
                audio.currentTime = Math.max(0, audio.currentTime - (details.seekOffset || 10));
            },
            seekforward: (details) => {
                audio.currentTime = Math.min(audio.duration || Infinity, audio.currentTime + (details.seekOffset || 10));
            },
            seekto: (details) => {
                if (Number.isFinite(details.seekTime)) audio.currentTime = details.seekTime;
            },
            stop: stopAndClose
        };

        Object.entries(actions).forEach(([action, handler]) => {
            try {
                navigator.mediaSession.setActionHandler(action, handler);
            } catch (_) {
                // Browsers expose different subsets of Media Session actions.
            }
        });
    }

    function restoreState() {
        let saved;
        try {
            saved = JSON.parse(sessionStorage.getItem(STORAGE_KEY) || 'null');
        } catch (_) {
            return;
        }
        if (!saved?.src) return;

        setMetadata(saved.metadata || {});
        setDockVisible(true);
        audio.src = saved.src;
        audio.playbackRate = saved.playbackRate || 1;
        audio.addEventListener('loadedmetadata', () => {
            if (Number.isFinite(saved.currentTime)) {
                audio.currentTime = Math.min(saved.currentTime, audio.duration || saved.currentTime);
            }
            updateUi();

            const recentlyNavigated = Date.now() - (saved.updatedAt || 0) < 15000;
            if (saved.wasPlaying && recentlyNavigated) {
                audio.play().catch(() => updateUi());
            }
        }, { once: true });
        audio.load();
    }

    function init() {
        audio = document.getElementById('scanner-global-audio');
        dock = document.getElementById('scanner-global-player');
        titleEl = document.getElementById('scanner-player-title');
        subtitleEl = document.getElementById('scanner-player-subtitle');
        badgeEl = document.getElementById('scanner-player-badge');
        toggleBtn = document.getElementById('scanner-player-toggle');
        playIcon = toggleBtn?.querySelector('.scanner-player-play-icon');
        pauseIcon = toggleBtn?.querySelector('.scanner-player-pause-icon');
        progressEl = document.getElementById('scanner-player-progress');
        currentEl = document.getElementById('scanner-player-current');
        durationEl = document.getElementById('scanner-player-duration');
        speedBtn = document.getElementById('scanner-player-speed');
        closeBtn = document.getElementById('scanner-player-close');
        if (!audio || !dock || !toggleBtn || !progressEl) return;

        toggleBtn.addEventListener('click', () => {
            if (!audio.src) return;
            if (audio.paused) audio.play().catch(() => {});
            else audio.pause();
        });

        progressEl.addEventListener('input', () => {
            seekRatio(Number(progressEl.value) / 1000);
        });

        speedBtn.addEventListener('click', () => {
            const currentIndex = SPEEDS.indexOf(audio.playbackRate);
            audio.playbackRate = SPEEDS[(currentIndex + 1) % SPEEDS.length];
            updateUi();
            persistState();
        });

        closeBtn.addEventListener('click', stopAndClose);
        document.addEventListener('click', handleExistingPlayerClick, true);

        ['play', 'pause', 'loadedmetadata', 'durationchange', 'ratechange', 'ended', 'error'].forEach((eventName) => {
            audio.addEventListener(eventName, () => {
                updateUi();
                persistState();
            });
        });
        audio.addEventListener('timeupdate', () => {
            updateUi();
            const currentSecond = Math.floor(audio.currentTime || 0);
            if (currentSecond !== lastSavedSecond) {
                lastSavedSecond = currentSecond;
                persistState();
            }
        });
        window.addEventListener('pagehide', persistState);

        registerMediaSessionActions();
        restoreState();
    }

    window.ScannerPlayer = {
        play: playItem,
        seekRatio,
        stop: stopAndClose,
        getState
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
