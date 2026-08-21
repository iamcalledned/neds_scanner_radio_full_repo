(() => {
  function decode(waveform) {
    if (!waveform || waveform.encoding !== 'uint8-base64' || !waveform.peaks) return null;
    try {
      const binary = window.atob(waveform.peaks);
      if (!binary.length) return null;
      const peaks = new Uint8Array(binary.length);
      for (let index = 0; index < binary.length; index += 1) {
        peaks[index] = binary.charCodeAt(index);
      }
      return peaks;
    } catch (error) {
      console.warn('[Waveform] Invalid stored peaks:', error);
      return null;
    }
  }

  function fromCall(call) {
    return call?.waveform || call?.metadata?.extra?.waveform || null;
  }

  function attach(element, waveform) {
    if (element) element._scannerWaveformPeaks = decode(waveform);
    return element?._scannerWaveformPeaks || null;
  }

  function fromElement(element) {
    if (!element) return null;
    if (element._scannerWaveformPeaks) return element._scannerWaveformPeaks;
    const encoded = element.dataset.waveformPeaks;
    if (!encoded) return null;
    return attach(element, {
      encoding: 'uint8-base64',
      peaks: encoded
    });
  }

  function draw(canvas, peaks, isFire, progress = 0) {
    if (!canvas || !peaks?.length) return false;
    const width = canvas.parentElement?.offsetWidth || canvas.offsetWidth || canvas.width || 280;
    canvas.width = width;
    const height = canvas.height;
    const context = canvas.getContext('2d');
    context.clearRect(0, 0, width, height);

    const barWidth = 3;
    const gap = 1;
    const step = barWidth + gap;
    const bars = Math.max(1, Math.floor(width / step));
    const playedX = Math.max(0, Math.min(1, Number(progress) || 0)) * width;
    const lit = isFire ? 'rgba(248,113,113,0.9)' : 'rgba(56,189,248,0.9)';
    const dim = isFire ? 'rgba(248,113,113,0.24)' : 'rgba(56,189,248,0.24)';

    for (let index = 0; index < bars; index += 1) {
      const start = Math.floor(index * peaks.length / bars);
      const end = Math.max(start + 1, Math.floor((index + 1) * peaks.length / bars));
      let peak = 0;
      for (let point = start; point < end && point < peaks.length; point += 1) {
        peak = Math.max(peak, peaks[point]);
      }
      const barHeight = Math.max(2, (peak / 255) * height * 0.9);
      const x = index * step;
      context.fillStyle = x <= playedX ? lit : dim;
      context.fillRect(x, (height - barHeight) / 2, barWidth, barHeight);
    }

    if (playedX > 0 && playedX < width) {
      context.fillStyle = 'rgba(255,255,255,0.72)';
      context.fillRect(Math.floor(playedX), 0, 1, height);
    }
    return true;
  }

  window.ScannerWaveform = { attach, decode, draw, fromCall, fromElement };
})();
