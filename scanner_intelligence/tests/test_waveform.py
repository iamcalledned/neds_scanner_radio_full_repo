import math
import struct
import tempfile
import unittest
import wave
from pathlib import Path

from shared.waveform import decode_waveform_peaks, extract_waveform


class WaveformExtractionTest(unittest.TestCase):
    def test_extracts_compact_envelope_from_real_pcm(self) -> None:
        sample_rate = 16000
        seconds = 2
        samples = []
        for index in range(sample_rate * seconds):
            time = index / sample_rate
            if time < 0.65:
                amplitude = 0
            elif time < 1.35:
                amplitude = int(18000 * math.sin(2 * math.pi * 440 * time))
            else:
                amplitude = 0
            samples.append(amplitude)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "speech-shape.wav"
            with wave.open(str(path), "wb") as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(sample_rate)
                wav_file.writeframes(struct.pack(f"<{len(samples)}h", *samples))

            waveform = extract_waveform(path, points=128)

        peaks = decode_waveform_peaks(waveform)
        self.assertEqual(waveform["version"], 1)
        self.assertEqual(waveform["encoding"], "uint8-base64")
        self.assertEqual(len(peaks), 128)
        self.assertAlmostEqual(waveform["duration_seconds"], seconds, places=2)
        self.assertLess(max(peaks[:30]), 10)
        self.assertGreater(max(peaks[45:85]), 240)
        self.assertLess(max(peaks[-30:]), 10)

    def test_rejects_non_pcm16_wave(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "eight-bit.wav"
            with wave.open(str(path), "wb") as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(1)
                wav_file.setframerate(8000)
                wav_file.writeframes(bytes([128] * 100))

            with self.assertRaisesRegex(ValueError, "16-bit PCM"):
                extract_waveform(path)


if __name__ == "__main__":
    unittest.main()
