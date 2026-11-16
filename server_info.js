"use strict";

// SERVER INFO 카드 전용 스크립트. index.html의 인코더 카드/스케줄러 로직과 분리해 따로 관리한다.
// index.html은 이 파일을 <script src="server_info.js"> 로 불러오며, escapeHtml()은 index.html의
// 인라인 스크립트가 정의한 전역 함수를 그대로 사용한다 (두 스크립트 다 로드가 끝난 뒤에야
// SSE 이벤트로 실제 호출되므로 로드 순서는 문제되지 않는다).

(function injectServerInfoStyles() {
    const style = document.createElement("style");
    style.textContent = `
        .si-section-title {
            font-size: 11px; color: #888; margin: 10px 0 4px; letter-spacing: 0.5px;
        }
        /* CPU 코어 그래프와 RAM/TRANSFER MODE를 한 줄에 나란히 배치 */
        .si-cpu-row { display: flex; gap: 16px; align-items: flex-start; }
        .si-cpu-left { flex: 1 1 auto; min-width: 0; }
        .si-cpu-right { flex: none; text-align: right; white-space: nowrap; }
        /* 로그는 평소 접혀 있다가 버튼으로 펼친다 */
        .si-log-toggle-btn {
            background: #333; color: #ccc; border: 1px solid #444; border-radius: 4px;
            padding: 3px 10px; font-size: 11px; cursor: pointer; margin-right: 6px;
        }
        .si-log-toggle-btn:hover { background: #3e3e3e; }
        #si-log { display: none; }
        #si-log.si-log-open { display: block; }

        /* 코어/스레드별 CPU 이용률 막대 */
        .si-cpu-cores { display: flex; flex-wrap: wrap; gap: 3px; align-items: flex-end; height: 40px; margin-top: 2px; }
        .si-core-bar {
            width: 9px; height: 100%; background: #111; position: relative;
            border-radius: 1px; overflow: hidden; flex: none;
        }
        .si-core-fill {
            position: absolute; bottom: 0; left: 0; width: 100%; height: 0%;
            background: linear-gradient(to top, #00c853 0%, #00c853 70%, #ffd600 70%, #ffd600 90%, #ff1744 90%, #ff1744 100%);
            transition: height 0.3s ease;
        }

        /* GPU 패널 */
        .si-gpu-card { background: #1a1a1a; border: 1px solid #333; border-radius: 4px; padding: 8px; margin-top: 4px; }
        .si-gpu-name { color: #ffca28; font-size: 12px; margin-bottom: 4px; }
        .si-gpu-row { display: flex; align-items: center; gap: 6px; font-size: 11px; color: #ccc; margin: 2px 0; }
        .si-gpu-row .si-gpu-label { width: 56px; flex: none; color: #999; }
        .si-mini-bar { flex: 1; height: 8px; background: #111; border-radius: 3px; overflow: hidden; }
        .si-mini-fill { height: 100%; background: #4fc3f7; width: 0%; transition: width 0.3s ease; }
        .si-gpu-extra { font-size: 11px; color: #aaa; margin-top: 4px; }

        /* 네트워크 인터페이스 패널 */
        .si-net-row {
            display: flex; justify-content: space-between; font-size: 12px; color: #ccc;
            padding: 2px 0; border-bottom: 1px dashed #2a2a2a;
        }
        .si-net-row:last-child { border-bottom: none; }
        .si-net-name { color: #fff; }
        .si-net-ip { color: #888; font-size: 10px; }
        .si-net-rate-up { color: #ff8a65; }
        .si-net-rate-down { color: #4fc3f7; }
    `;
    document.head.appendChild(style);
})();

function createServerInfoCard() {
    const wrapper = document.getElementById("encoders-wrapper");
    const div = document.createElement("div");
    div.className = "encoder-card border-free";
    div.id = "server-info-card";
    div.innerHTML = `
        <div class="enc-header">
            <span class="enc-name">SERVER INFO</span>
            <span>
                <button id="si-log-toggle-btn" class="si-log-toggle-btn" onclick="toggleServerLog()">로그 보기 ▾</button>
                <button id="si-log-copy-btn" class="btn-copy" onclick="copyServerLog()">LOG 복사</button>
            </span>
        </div>
        <div style="font-family: monospace; font-size: 13px; line-height: 1.8; color: #ccc;">
            HOST: <span id="si-hostname" style="color: #fff;">-</span>
            <span style="color: #888; font-size: 11px;">
                (host up <span id="si-host-uptime" style="color: #aaa;">-</span> ·
                svcr2026 up <span id="si-service-uptime" style="color: #aaa;">-</span>)
            </span><br>
            <div class="si-cpu-row">
                <div class="si-cpu-left">
                    CPU: <span id="si-cpu" style="color: #fff;">-</span>% (<span id="si-cpu-model" style="color: #fff;">-</span> / <span id="si-cores" style="color: #fff;">-</span> cores)
                    <div class="si-cpu-cores" id="si-cpu-cores"></div>
                </div>
                <div class="si-cpu-right">
                    RAM: <span id="si-ram-free" style="color: #fff;">-</span> / <span id="si-ram-total" style="color: #fff;">-</span> GB<br>
                    TRANSFER MODE: <span id="si-transfer-mode" style="color: #ffca28;">-</span>
                </div>
            </div>

            <div class="si-section-title">GPU</div>
            <div id="si-gpu-list"><span style="color:#666;">(nvidia-smi 사용 불가 또는 조회 중)</span></div>

            <div class="si-section-title">NETWORK (IPv4 보유 NIC)</div>
            <div id="si-net-list"><span style="color:#666;">(조회 중)</span></div>
        </div>
        <div id="si-log" style="margin-top: 10px; height: 300px; overflow-y: auto; background: #0d0d0d; border: 1px solid #282828; border-radius: 4px; padding: 8px; font-family: monospace; font-size: 11px; color: #9ccc65; white-space: pre-wrap;"></div>
    `;
    wrapper.appendChild(div);
    serverInfoCardEl = div;
}

// [추가] 로그 창 펼침/접힘. 평소엔 접혀 있고(기본값), 버튼으로 토글한다.
let serverLogOpen = false;

function toggleServerLog() {
    serverLogOpen = !serverLogOpen;
    const logEl = document.getElementById("si-log");
    const btn = document.getElementById("si-log-toggle-btn");
    if (logEl) logEl.classList.toggle("si-log-open", serverLogOpen);
    if (btn) btn.innerText = serverLogOpen ? "로그 숨기기 ▴" : "로그 보기 ▾";
}

// [추가] 코어/스레드별 CPU 이용률 막대 렌더링. 최초 호출 시 막대 개수(cpu_cores)만큼 DOM을
// 한 번만 만들고, 이후에는 각 막대의 height만 갱신한다 (매초 innerHTML을 통째로 다시 만들지 않음).
function updateCpuCoresBars(percpu) {
    const container = document.getElementById("si-cpu-cores");
    if (!container || !Array.isArray(percpu)) return;

    if (container.children.length !== percpu.length) {
        container.innerHTML = percpu.map((_, i) => `
            <div class="si-core-bar" id="si-core-${i}" title="Core ${i}">
                <div class="si-core-fill" id="si-core-fill-${i}"></div>
            </div>
        `).join('');
    }

    percpu.forEach((pct, i) => {
        const fill = document.getElementById(`si-core-fill-${i}`);
        const bar = document.getElementById(`si-core-${i}`);
        if (fill) fill.style.height = Math.min(100, Math.max(0, pct)).toFixed(0) + '%';
        if (bar) bar.title = `Core ${i}: ${pct.toFixed(1)}%`;
    });
}

// [추가] nvidia-smi 기반 GPU 패널. GPU가 없거나 nvidia-smi를 못 쓰는 호스트에서는
// gpu_stats가 빈 배열로 오므로 안내 문구만 표시한다.
function updateGpuPanel(gpuStats) {
    const listEl = document.getElementById("si-gpu-list");
    if (!listEl) return;
    if (!gpuStats || gpuStats.length === 0) {
        listEl.innerHTML = `<span style="color:#666;">(nvidia-smi로 조회된 GPU 없음)</span>`;
        return;
    }
    listEl.innerHTML = gpuStats.map(gpu => {
        const bar = (label, pct) => `
            <div class="si-gpu-row">
                <span class="si-gpu-label">${label}</span>
                <div class="si-mini-bar"><div class="si-mini-fill" style="width:${pct ?? 0}%;"></div></div>
                <span>${pct != null ? pct.toFixed(0) + '%' : '-'}</span>
            </div>`;
        const vram = (gpu.mem_used_mb != null && gpu.mem_total_mb != null)
            ? `${(gpu.mem_used_mb / 1024).toFixed(1)} / ${(gpu.mem_total_mb / 1024).toFixed(1)} GB`
            : '-';
        const power = (gpu.power_draw_w != null)
            ? `${gpu.power_draw_w.toFixed(0)}${gpu.power_limit_w != null ? ' / ' + gpu.power_limit_w.toFixed(0) : ''} W`
            : '-';
        const temp = gpu.temp_c != null ? `${gpu.temp_c.toFixed(0)}°C` : '-';
        return `
            <div class="si-gpu-card">
                <div class="si-gpu-name">GPU${gpu.index}: ${escapeHtml(gpu.name || '?')}</div>
                ${bar('GPU', gpu.gpu_util)}
                ${bar('NVENC', gpu.encoder_util)}
                ${bar('NVDEC', gpu.decoder_util)}
                <div class="si-gpu-extra">전력: ${power} · 온도: ${temp} · VRAM: ${vram}</div>
            </div>`;
    }).join('');
}

// [추가] IPv4를 가진 모든 네트워크 인터페이스의 순간 송/수신 속도(Mbps). main.py가 매 tick마다
// 직전 호출과의 바이트 카운터 델타로 계산해서 보내주므로, 여기서는 그대로 표시만 한다.
function updateNetPanel(netIo) {
    const listEl = document.getElementById("si-net-list");
    if (!listEl) return;
    if (!netIo || netIo.length === 0) {
        listEl.innerHTML = `<span style="color:#666;">(IPv4를 가진 NIC 없음)</span>`;
        return;
    }
    listEl.innerHTML = netIo.map(nic => `
        <div class="si-net-row">
            <span>
                <span class="si-net-name">${escapeHtml(nic.name)}</span>
                <span class="si-net-ip">(${(nic.ipv4 || []).join(', ')})</span>
            </span>
            <span>
                <span class="si-net-rate-up">↑ ${nic.tx_mbps.toFixed(2)}</span> /
                <span class="si-net-rate-down">↓ ${nic.rx_mbps.toFixed(2)}</span> Mbps
            </span>
        </div>
    `).join('');
}

function updateServerInfoCard(info) {
    document.getElementById("si-hostname").innerText = info.hostname;
    document.getElementById("si-host-uptime").innerText = info.host_uptime || '-';
    document.getElementById("si-service-uptime").innerText = info.service_uptime || '-';
    document.getElementById("si-cpu").innerText = info.cpu_percent.toFixed(1);
    // [수정] %만 보여주던 걸 free/total(GB)로 변경 - 운영 중 "실제로 메모리가 얼마나 남았는지"가
    // 백분율보다 바로 와닿는다.
    document.getElementById("si-ram-free").innerText = info.mem_free_gb.toFixed(1);
    document.getElementById("si-ram-total").innerText = info.mem_total_gb.toFixed(1);
    document.getElementById("si-cpu-model").innerText = info.cpu_model;
    document.getElementById("si-cores").innerText = info.cpu_cores;
    // [추가] config.json의 전역 bmxtranswrap 설정으로 결정되는 transfer 방식 표시
    document.getElementById("si-transfer-mode").innerText = info.transfer_mode || '-';

    updateCpuCoresBars(info.cpu_percpu);
    updateGpuPanel(info.gpu_stats);
    updateNetPanel(info.net_io);

    const hostnameDisplay = document.getElementById("hostname-display");
    if (hostnameDisplay) hostnameDisplay.innerText = `(${info.hostname})`;

    // [추가] 브라우저 탭 타이틀을 sVCR2026-호스트명 으로 표시
    document.title = `sVCR2026-${info.hostname}`;

    // [추가] main.py 마지막 수정일시를 버전 표기로 상단에 표시
    const versionDisplay = document.getElementById("version-display");
    if (versionDisplay) versionDisplay.innerText = `version - ${info.main_py_version || '-'}`;

    // [추가] 스케줄러 패널 타이틀 옆에 서버 시각 표시 (브라우저 로컬 시계가 아닌 실제 서버 시계 기준 -
    // 스케줄 시작/종료 판단이 서버 시계로 이뤄지므로 이걸 봐야 혼동이 없다)
    const schedTimeEl = document.getElementById("sched-server-time");
    if (schedTimeEl) schedTimeEl.innerText = info.server_time || '-';

    const logEl = document.getElementById("si-log");
    if (logEl) {
        // [수정] 사용자가 로그를 위에서 아래로(과거 로그 쪽으로) 스크롤해서 보고 있는 중이면
        // 매초 갱신 때마다 강제로 맨 위로 되돌리지 않는다. 이미 맨 위(최신 로그)를 보고 있을
        // 때만 갱신 후에도 맨 위를 유지한다.
        const wasAtTop = logEl.scrollTop <= 4;
        // 최근 2000줄(기존 500줄의 4배) 중 마지막 로그가 제일 위로 오도록 역순 표시
        const lines = (info.log_lines || []).slice(-2000).reverse();
        // [추가] 자동 재시작 로그([AUTO-RESTART] 태그)는 다른 로그보다 눈에 띄게 강조
        // [추가] STREAM/REC/STOP 버튼 조작 로그("API Action 수신")도 ffmpeg 내부 통계 등
        // 잡다한 로그 사이에 묻혀 잘 안 보인다는 지적이 있어, 파란색으로 구분되게 강조한다.
        logEl.innerHTML = lines.map(line => {
            const escaped = escapeHtml(line);
            if (line.includes('[AUTO-RESTART]')) {
                return `<span style="display:block; color:#ff5252; font-weight:bold; background:rgba(255,82,82,0.12); border-left:3px solid #ff5252; padding:1px 4px; margin:1px 0;">${escaped}</span>`;
            }
            if (line.includes('API Action 수신')) {
                return `<span style="display:block; color:#4fc3f7; font-weight:bold; background:rgba(79,195,247,0.10); border-left:3px solid #4fc3f7; padding:1px 4px; margin:1px 0;">${escaped}</span>`;
            }
            return escaped;
        }).join('\n');
        if (wasAtTop) logEl.scrollTop = 0;
    }
}

// [추가] SERVER INFO 로그 클립보드 복사
function fallbackCopyText(text, onDone) {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    try {
        document.execCommand('copy');
        if (onDone) onDone();
    } catch (e) {
        console.error('클립보드 복사 실패:', e);
    }
    document.body.removeChild(ta);
}

function copyServerLog() {
    const logEl = document.getElementById("si-log");
    const btn = document.getElementById("si-log-copy-btn");
    if (!logEl) return;
    const text = logEl.innerText;

    const showCopied = () => {
        if (!btn) return;
        const original = btn.innerText;
        btn.innerText = "복사됨!";
        setTimeout(() => { btn.innerText = original; }, 1500);
    };

    // HTTP(비보안 컨텍스트)로 서빙되는 경우 Clipboard API를 못 쓸 수 있어 fallback 처리
    if (window.isSecureContext && navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(showCopied).catch(() => fallbackCopyText(text, showCopied));
    } else {
        fallbackCopyText(text, showCopied);
    }
}
