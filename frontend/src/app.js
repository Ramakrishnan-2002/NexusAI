// WikiPulse Frontend Application Logic
let eventSource = null;
let currentTab = 'pulse';

document.addEventListener('DOMContentLoaded', () => {
    lucide.createIcons();
    initLiveStream();
    startMetricsPolling();
    loadTrends();
    loadHotArticles();
});

// Tab Navigation
function switchTab(tabId) {
    currentTab = tabId;
    document.querySelectorAll('.view-panel').forEach(el => el.classList.add('hidden'));
    document.querySelectorAll('.nav-tab').forEach(el => {
        el.classList.remove('active', 'bg-indigo-600', 'text-white');
        el.classList.add('text-slate-400');
    });

    const targetView = document.getElementById(`view-${tabId}`);
    if (targetView) targetView.classList.remove('hidden');

    const targetTabBtn = document.getElementById(`tab-${tabId}`);
    if (targetTabBtn) {
        targetTabBtn.classList.add('active', 'bg-indigo-600', 'text-white');
        targetTabBtn.classList.remove('text-slate-400');
    }

    if (tabId === 'trends') loadTrends();
    if (tabId === 'pulse') loadHotArticles();
    lucide.createIcons();
}

// SSE Live Stream Ingestion
function initLiveStream() {
    const streamContainer = document.getElementById('events-stream-list');
    const statusBadge = document.getElementById('stream-status');

    try {
        eventSource = new EventSource('/api/v1/stream/live');

        eventSource.addEventListener('recent_change', (e) => {
            const data = JSON.parse(e.data);
            appendStreamEvent(data);
        });

        eventSource.onopen = () => {
            statusBadge.innerText = 'Stream Active';
        };

        eventSource.onerror = () => {
            statusBadge.innerText = 'Reconnecting...';
        };
    } catch (err) {
        console.warn('SSE stream error:', err);
    }
}

function appendStreamEvent(evt) {
    const list = document.getElementById('events-stream-list');
    if (!list) return;

    const div = document.createElement('div');
    div.className = 'px-4 py-3 hover:bg-slate-800/40 transition flex items-start justify-between space-x-3';

    const diffColor = evt.byte_diff > 0 ? 'text-emerald-400' : (evt.byte_diff < 0 ? 'text-rose-400' : 'text-slate-400');
    const diffSign = evt.byte_diff > 0 ? `+${evt.byte_diff}` : `${evt.byte_diff}`;
    const botBadge = evt.is_bot ? '<span class="px-1.5 py-0.2 text-[10px] bg-amber-500/20 text-amber-300 rounded border border-amber-500/30">BOT</span>' : '';

    div.innerHTML = `
        <div class="flex-1 min-w-0">
            <div class="flex items-center space-x-2">
                <span class="font-bold text-slate-100 truncate">${escapeHtml(evt.article_title || 'Untitled')}</span>
                ${botBadge}
                <span class="text-slate-500 text-[11px]">${escapeHtml(evt.wiki || 'enwiki')}</span>
            </div>
            <p class="text-slate-400 text-[11px] truncate mt-0.5">${escapeHtml(evt.comment || 'No edit summary provided')}</p>
        </div>
        <div class="text-right shrink-0">
            <div class="font-bold ${diffColor} text-[11px]">${diffSign} B</div>
            <div class="text-slate-500 text-[10px]">${escapeHtml(evt.editor_username || 'Anonymous')}</div>
        </div>
    `;

    list.insertBefore(div, list.firstChild);

    // Limit DOM node count for performance
    if (list.children.length > 50) {
        list.removeChild(list.lastChild);
    }
}

// Metrics Telemetry Polling
function startMetricsPolling() {
    const fetchMetrics = async () => {
        try {
            const res = await fetch('/api/v1/metrics');
            if (res.ok) {
                const data = await res.json();
                document.getElementById('kpi-processed').innerText = data.processed_count.toLocaleString();
                document.getElementById('kpi-rate').innerText = data.events_per_sec.toFixed(1);
                document.getElementById('kpi-spikes').innerText = data.trend_count;
                document.getElementById('kpi-cache-hit').innerText = `${data.redis_hit_ratio_percent}%`;
                document.getElementById('kpi-latency').innerText = `${data.api_p95_latency_ms} ms`;
                document.getElementById('kpi-fallbacks').innerText = data.llm_fallbacks;
                document.getElementById('kpi-dlq').innerText = data.dlq_count;
            }
        } catch (err) {
            console.error('Metrics fetch error:', err);
        }
    };

    fetchMetrics();
    setInterval(fetchMetrics, 3000);
}

// Hot Articles List
async function loadHotArticles() {
    const list = document.getElementById('hot-articles-list');
    if (!list) return;

    try {
        const res = await fetch('/api/v1/articles?limit=10');
        if (res.ok) {
            const articles = await res.json();
            list.innerHTML = '';
            articles.forEach((art, idx) => {
                const item = document.createElement('div');
                item.className = 'p-3 hover:bg-slate-800/40 rounded-lg transition flex items-center justify-between';
                item.innerHTML = `
                    <div class="flex items-center space-x-3">
                        <span class="text-xs font-bold text-indigo-400 w-4">${idx + 1}</span>
                        <div>
                            <div class="text-xs font-semibold text-white">${escapeHtml(art.title)}</div>
                            <div class="text-[10px] text-slate-500">${art.wiki} &bull; Namespace ${art.namespace}</div>
                        </div>
                    </div>
                    <div class="text-right">
                        <span class="px-2 py-0.5 text-[10px] font-semibold bg-indigo-500/10 text-indigo-300 rounded border border-indigo-500/20">Active</span>
                    </div>
                `;
                list.appendChild(item);
            });
        }
    } catch (err) {
        console.error('Hot articles error:', err);
    }
}

// Activity Spikes & Trends
async function loadTrends() {
    const grid = document.getElementById('trends-cards-grid');
    if (!grid) return;

    try {
        const res = await fetch('/api/v1/trends?limit=20');
        if (res.ok) {
            const data = await res.json();
            grid.innerHTML = '';

            if (data.trends.length === 0) {
                grid.innerHTML = `
                    <div class="col-span-full py-12 text-center text-slate-500 text-xs">
                        No activity spikes detected yet. Spikes appear automatically when edit velocity exceeds 3.0x baseline.
                    </div>
                `;
                return;
            }

            data.trends.forEach(trend => {
                const card = document.createElement('div');
                card.className = 'bg-slate-900 border border-slate-800 hover:border-indigo-500/50 p-5 rounded-xl transition shadow-sm flex flex-col justify-between';

                const summaryHtml = trend.ai_summary 
                    ? `<p class="text-xs text-slate-300 mt-3 p-2.5 bg-slate-950 rounded-lg border border-slate-800/80 leading-relaxed">${escapeHtml(trend.ai_summary)}</p>` 
                    : `<button onclick="analyzeTrend(${trend.id}, this)" class="mt-3 w-full py-1.5 bg-indigo-600/20 hover:bg-indigo-600/30 text-indigo-300 text-xs font-semibold rounded-lg border border-indigo-500/30 transition flex items-center justify-center space-x-1.5">
                        <i data-lucide="sparkles" class="w-3.5 h-3.5"></i>
                        <span>Generate AI Intelligence</span>
                    </button>`;

                card.innerHTML = `
                    <div>
                        <div class="flex items-center justify-between mb-2">
                            <span class="px-2 py-0.5 text-[10px] font-bold bg-amber-500/20 text-amber-400 rounded border border-amber-500/30">${trend.spike_multiplier.toFixed(1)}x Velocity Spike</span>
                            <span class="text-[10px] text-slate-500">${new Date(trend.first_detected_at).toLocaleTimeString()}</span>
                        </div>
                        <h3 class="text-sm font-bold text-white mb-1">${escapeHtml(trend.article_title)}</h3>
                        <div class="flex items-center space-x-4 text-xs text-slate-400 mt-2">
                            <div>Score: <b class="text-white">${trend.activity_score.toFixed(1)}</b></div>
                            <div>Velocity: <b class="text-white">${trend.edits_per_minute.toFixed(1)}/min</b></div>
                            <div>Editors: <b class="text-white">${trend.unique_editors}</b></div>
                        </div>
                        ${summaryHtml}
                    </div>
                `;
                grid.appendChild(card);
            });
            lucide.createIcons();
        }
    } catch (err) {
        console.error('Trends load error:', err);
    }
}

// Trigger AI Trend Analysis
async function analyzeTrend(trendId, btnElement) {
    btnElement.disabled = true;
    btnElement.innerHTML = `<span class="animate-spin mr-1">&bull;</span> Synthesizing Evidence...`;

    try {
        const res = await fetch(`/api/v1/trends/${trendId}/analyze`, { method: 'POST' });
        if (res.ok) {
            loadTrends();
        } else {
            btnElement.innerText = 'Failed. Try Again';
            btnElement.disabled = false;
        }
    } catch (err) {
        console.error('Analyze trend error:', err);
        btnElement.innerText = 'Error';
    }
}

// RAG Hybrid Search & QA Submission
async function handleAskSubmit(event) {
    event.preventDefault();
    const query = document.getElementById('rag-query-input').value.trim();
    if (!query) return;

    const provider = document.getElementById('rag-provider-select').value;
    const topK = parseInt(document.getElementById('rag-topk-select').value) || 5;
    const submitBtn = document.getElementById('btn-rag-submit');
    const container = document.getElementById('rag-response-container');

    submitBtn.disabled = true;
    submitBtn.innerHTML = `<span class="animate-spin mr-1">&bull;</span> Retrieving &amp; Reasoning...`;

    try {
        const res = await fetch('/api/v1/ai/ask', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                question: query,
                top_k_evidence: topK,
                provider_override: provider || null,
            })
        });

        if (res.ok) {
            const data = await res.json();
            renderRAGResponse(data);
            container.classList.remove('hidden');
        } else {
            alert('Failed to retrieve intelligence answer from LLM Gateway.');
        }
    } catch (err) {
        console.error('RAG QA error:', err);
    } finally {
        submitBtn.disabled = false;
        submitBtn.innerHTML = `<i data-lucide="search" class="w-3.5 h-3.5"></i><span>Ask WikiPulse</span>`;
        lucide.createIcons();
    }
}

function renderRAGResponse(data) {
    document.getElementById('rag-model-badge').innerText = `${data.provider_used.toUpperCase()} (${data.model_used})`;
    document.getElementById('rag-timing-badge').innerText = `Retrieval: ${data.retrieval_took_ms}ms | LLM: ${data.llm_took_ms}ms | Total: ${data.total_took_ms}ms`;
    document.getElementById('rag-answer-text').innerText = data.answer;

    // Evidence Points
    const evidenceList = document.getElementById('rag-evidence-points');
    evidenceList.innerHTML = '';
    const points = data.structured_analysis.evidence_points || [];
    if (points.length === 0) {
        evidenceList.innerHTML = '<li>Grounded directly on recent indexed revision changes.</li>';
    } else {
        points.forEach(p => {
            const li = document.createElement('li');
            li.innerText = p;
            evidenceList.appendChild(li);
        });
    }

    // Citations
    const citationsGrid = document.getElementById('rag-citations-grid');
    citationsGrid.innerHTML = '';
    (data.citations || []).forEach(c => {
        const div = document.createElement('div');
        div.className = 'p-3 bg-slate-950 border border-slate-800 rounded-lg text-xs';
        div.innerHTML = `
            <div class="font-bold text-indigo-400 mb-1 flex items-center justify-between">
                <span>${escapeHtml(c.article_title)}</span>
                <span class="text-[10px] text-slate-500 font-mono">Rev: ${c.revision_id || 'N/A'}</span>
            </div>
            <p class="text-slate-400 text-[11px] italic">"${escapeHtml(c.snippet)}"</p>
        `;
        citationsGrid.appendChild(div);
    });
}

function escapeHtml(text) {
    if (!text) return '';
    const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
    return String(text).replace(/[&<>"']/g, m => map[m]);
}
