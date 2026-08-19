/**
 * Real-Time Sentiment Stream Dashboard Client
 * Handles WebSockets, Chart.js Visualizations, REST Invocations, and Live Feeds
 */

document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const kpiTotal = document.getElementById('kpiTotal');
    const kpiPositive = document.getElementById('kpiPositive');
    const kpiNegative = document.getElementById('kpiNegative');
    const kpiNeutral = document.getElementById('kpiNeutral');
    const kpiPosPct = document.getElementById('kpiPosPct');
    const kpiNegPct = document.getElementById('kpiNegPct');
    const kpiNeuPct = document.getElementById('kpiNeuPct');
    const kpiPolarity = document.getElementById('kpiPolarity');
    const posProgressBar = document.getElementById('posProgressBar');
    const negProgressBar = document.getElementById('negProgressBar');
    const neuProgressBar = document.getElementById('neuProgressBar');
    const polarityPointer = document.getElementById('polarityPointer');
    const throughputValue = document.getElementById('throughputValue');
    
    const streamTableBody = document.getElementById('streamTableBody');
    const feedCountTag = document.getElementById('feedCountTag');
    const keywordsContainer = document.getElementById('keywordsContainer');

    const connStatusBadge = document.getElementById('connStatusBadge');
    const connStatusText = document.getElementById('connStatusText');
    const btnToggleStream = document.getElementById('btnToggleStream');
    const streamIcon = document.getElementById('streamIcon');
    const streamBtnText = document.getElementById('streamBtnText');
    const btnClearFeed = document.getElementById('btnClearFeed');

    const customTextInput = document.getElementById('customTextInput');
    const btnSendText = document.getElementById('btnSendText');
    const presetChips = document.querySelectorAll('.chip-btn');
    const speedButtons = document.querySelectorAll('.speed-btn');

    let isStreamingActive = true;
    let ws = null;
    let rowCount = 0;
    const MAX_ROWS = 50;

    // -------------------------------------------------------------
    // Charts Initialization
    // -------------------------------------------------------------
    const donutCtx = document.getElementById('sentimentDonutChart').getContext('2d');
    const sentimentDonutChart = new Chart(donutCtx, {
        type: 'doughnut',
        data: {
            labels: ['Positive 😊', 'Negative 😞', 'Neutral 😐'],
            datasets: [{
                data: [0, 0, 0],
                backgroundColor: ['#10b981', '#ef4444', '#38bdf8'],
                borderColor: '#080c14',
                borderWidth: 3,
                hoverOffset: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '70%',
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: '#94a3b8', font: { family: 'Inter', size: 12 }, boxWidth: 12 }
                }
            }
        }
    });

    const lineCtx = document.getElementById('polarityTimelineChart').getContext('2d');
    const timelineData = {
        labels: [],
        datasets: [{
            label: 'Polarity (-1.0 to +1.0)',
            data: [],
            borderColor: '#6366f1',
            backgroundColor: 'rgba(99, 102, 241, 0.15)',
            borderWidth: 2,
            tension: 0.35,
            fill: true,
            pointRadius: 3,
            pointBackgroundColor: '#8b5cf6'
        }]
    };

    const polarityTimelineChart = new Chart(lineCtx, {
        type: 'line',
        data: timelineData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    min: -1.0,
                    max: 1.0,
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 } }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 }, maxTicksLimit: 6 }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });

    // -------------------------------------------------------------
    // WebSocket Connection
    // -------------------------------------------------------------
    function connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/stream`;

        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            connStatusBadge.style.background = 'rgba(16, 185, 129, 0.1)';
            connStatusBadge.style.color = '#10b981';
            connStatusText.innerText = 'Connected to Stream';
        };

        ws.onmessage = (event) => {
            try {
                const message = jsonSafeParse(event.data);
                if (!message) return;

                if (message.type === 'INITIAL_STATE') {
                    if (message.history && Array.isArray(message.history)) {
                        streamTableBody.innerHTML = '';
                        rowCount = 0;
                        message.history.forEach(item => addTableRow(item, false));
                    }
                    if (message.metrics) updateMetrics(message.metrics);
                } else if (message.type === 'NEW_EVENT') {
                    if (message.data) addTableRow(message.data, true);
                    if (message.metrics) updateMetrics(message.metrics);
                }
            } catch (err) {
                console.error('WS parse error:', err);
            }
        };

        ws.onclose = () => {
            connStatusBadge.style.background = 'rgba(239, 68, 68, 0.1)';
            connStatusBadge.style.color = '#ef4444';
            connStatusText.innerText = 'Reconnecting...';
            setTimeout(connectWebSocket, 3000);
        };

        ws.onerror = () => {
            ws.close();
        };
    }

    function jsonSafeParse(str) {
        try { return JSON.parse(str); } catch (e) { return null; }
    }

    // -------------------------------------------------------------
    // Update KPI & UI State
    // -------------------------------------------------------------
    function updateMetrics(m) {
        if (!m) return;
        kpiTotal.innerText = m.total.toLocaleString();
        kpiPositive.innerText = m.positive.toLocaleString();
        kpiNegative.innerText = m.negative.toLocaleString();
        kpiNeutral.innerText = m.neutral.toLocaleString();

        kpiPosPct.innerText = `${m.positive_pct}%`;
        kpiNegPct.innerText = `${m.negative_pct}%`;
        kpiNeuPct.innerText = `${m.neutral_pct}%`;

        posProgressBar.style.width = `${m.positive_pct}%`;
        negProgressBar.style.width = `${m.negative_pct}%`;
        neuProgressBar.style.width = `${m.neutral_pct}%`;

        kpiPolarity.innerText = (m.avg_polarity >= 0 ? '+' : '') + m.avg_polarity.toFixed(2);
        if (m.avg_polarity > 0.1) {
            kpiPolarity.style.color = '#10b981';
        } else if (m.avg_polarity < -0.1) {
            kpiPolarity.style.color = '#ef4444';
        } else {
            kpiPolarity.style.color = '#38bdf8';
        }

        // Move polarity dial pointer (-1.0 to 1.0 mapped to 0% to 100%)
        const pct = Math.max(0, Math.min(100, ((m.avg_polarity + 1.0) / 2.0) * 100));
        polarityPointer.style.left = `${pct}%`;

        throughputValue.innerText = `${m.throughput_eps} /s`;

        // Update Donut Chart
        sentimentDonutChart.data.datasets[0].data = [m.positive, m.negative, m.neutral];
        sentimentDonutChart.update();

        // Update Keywords
        if (m.top_keywords && m.top_keywords.length > 0) {
            keywordsContainer.innerHTML = m.top_keywords.map(([word, count]) => {
                const isNeg = word.includes('terrible') || word.includes('boring') || word.includes('hate') || word.includes('bad') || word.includes('not');
                return `<span class="keyword-pill ${isNeg ? 'neg' : ''}">${word} (x${count})</span>`;
            }).join('');
        }
    }

    // -------------------------------------------------------------
    // Append Live Table Row
    // -------------------------------------------------------------
    function addTableRow(eventData, isPrepend = true) {
        const row = document.createElement('tr');
        row.className = 'stream-row';

        let badgeClass = 'badge-neutral';
        if (eventData.sentiment === 'Positive') badgeClass = 'badge-positive';
        else if (eventData.sentiment === 'Negative') badgeClass = 'badge-negative';

        const polarityText = (eventData.polarity >= 0 ? '+' : '') + eventData.polarity.toFixed(2);
        const barColor = eventData.sentiment === 'Positive' ? '#10b981' : (eventData.sentiment === 'Negative' ? '#ef4444' : '#38bdf8');

        row.innerHTML = `
            <td class="time-cell">${eventData.timestamp || '00:00:00'}</td>
            <td class="text-cell">
                <div>${escapeHtml(eventData.text)}</div>
                <div class="text-source">Source: ${escapeHtml(eventData.source || 'Stream')} • ${escapeHtml(eventData.user || 'anon')}</div>
            </td>
            <td>
                <span class="badge-sentiment ${badgeClass}">
                    <span>${eventData.emoji}</span>
                    <span>${eventData.sentiment}</span>
                </span>
            </td>
            <td class="polarity-metric-cell">
                <div style="display:flex; justify-content:space-between;">
                    <span>Score: ${polarityText}</span>
                    <span style="color:#94a3b8;">${eventData.confidence}%</span>
                </div>
                <div class="conf-bar-mini">
                    <div class="conf-bar-fill" style="width: ${eventData.confidence}%; background: ${barColor};"></div>
                </div>
            </td>
        `;

        if (isPrepend) {
            streamTableBody.insertBefore(row, streamTableBody.firstChild);
            rowCount++;
            if (rowCount > MAX_ROWS) {
                streamTableBody.removeChild(streamTableBody.lastChild);
                rowCount--;
            }

            // Update Polarity Timeline Chart
            timelineData.labels.push(eventData.timestamp || '');
            timelineData.datasets[0].data.push(eventData.polarity);
            if (timelineData.labels.length > 25) {
                timelineData.labels.shift();
                timelineData.datasets[0].data.shift();
            }
            polarityTimelineChart.update('none');
        } else {
            streamTableBody.appendChild(row);
            rowCount++;
        }

        feedCountTag.innerText = `(${rowCount} items)`;
    }

    function escapeHtml(text) {
        if (!text) return '';
        const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
        return text.replace(/[&<>"']/g, m => map[m]);
    }

    // -------------------------------------------------------------
    // User Action: Send Custom Text
    // -------------------------------------------------------------
    async function sendCustomText(text) {
        if (!text || !text.trim()) return;
        try {
            const resp = await fetch('/api/predict', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    text: text.trim(),
                    source: 'UserInteractiveConsole',
                    user: 'saumya'
                })
            });
            if (resp.ok) {
                customTextInput.value = '';
            }
        } catch (err) {
            console.error('Failed to send text:', err);
        }
    }

    btnSendText.addEventListener('click', () => {
        sendCustomText(customTextInput.value);
    });

    customTextInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            sendCustomText(customTextInput.value);
        }
    });

    // Preset chip clicks
    presetChips.forEach(chip => {
        chip.addEventListener('click', () => {
            const text = chip.getAttribute('data-text');
            customTextInput.value = text;
            sendCustomText(text);
        });
    });

    // Toggle Stream Generator
    btnToggleStream.addEventListener('click', async () => {
        isStreamingActive = !isStreamingActive;
        try {
            await fetch('/api/stream/control', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled: isStreamingActive })
            });

            if (isStreamingActive) {
                streamIcon.innerText = '⏸';
                streamBtnText.innerText = 'Pause Stream';
                btnToggleStream.style.background = 'rgba(99, 102, 241, 0.15)';
            } else {
                streamIcon.innerText = '▶';
                streamBtnText.innerText = 'Resume Stream';
                btnToggleStream.style.background = 'rgba(16, 185, 129, 0.2)';
            }
        } catch (err) {
            console.error('Failed to toggle stream:', err);
        }
    });

    // Speed Selector Buttons
    speedButtons.forEach(btn => {
        btn.addEventListener('click', async () => {
            speedButtons.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            const speed = parseFloat(btn.getAttribute('data-speed'));
            try {
                await fetch('/api/stream/control', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ enabled: isStreamingActive, speed: speed })
                });
            } catch (err) {
                console.error('Failed to set speed:', err);
            }
        });
    });

    // Clear Feed
    btnClearFeed.addEventListener('click', async () => {
        streamTableBody.innerHTML = '';
        rowCount = 0;
        feedCountTag.innerText = `(0 items)`;
        timelineData.labels = [];
        timelineData.datasets[0].data = [];
        polarityTimelineChart.update();
        try {
            await fetch('/api/clear', { method: 'POST' });
        } catch (err) {
            console.error('Failed to clear:', err);
        }
    });

    // Initialize Connection
    connectWebSocket();
});
