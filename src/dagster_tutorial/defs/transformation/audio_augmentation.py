import os
import pathlib
import random
import math
import torch
import torchaudio

TARGET_SAMPLE_RATE = 16000
NOISE_DIR = "/home/baks/Documents/work/exploration/dagster-tutorial/noise_folder"


class ResampleAudio:
    def __init__(self, target_sample_rate):
        self.target_sample_rate = target_sample_rate

    def __call__(self, data):
        audio_data, original_sample_rate = data

        if original_sample_rate == self.target_sample_rate:
            return audio_data, self.target_sample_rate

        resampler = torchaudio.transforms.Resample(
            orig_freq=original_sample_rate, new_freq=self.target_sample_rate
        )
        resampled_audio = resampler(audio_data)
        return resampled_audio, self.target_sample_rate


class RandomSpeedChange:
    def __init__(self, sample_rate):
        self.sample_rate = sample_rate

    def __call__(self, data):
        audio_data, sample_rate = data

        speed_factor = random.choice([0.9, 1.0, 1.1])
        if speed_factor == 1.0:
            return audio_data, sample_rate

        sox_effects = [
            ["speed", str(speed_factor)],
            ["rate", str(self.sample_rate)],
        ]
        transformed_audio, _ = torchaudio.sox_effects.apply_effects_tensor(
            audio_data, self.sample_rate, sox_effects
        )
        return transformed_audio, sample_rate


class RandomBackgroundNoise:
    def __init__(self, sample_rate, noise_dir, min_snr_db=0, max_snr_db=15):
        self.sample_rate = sample_rate
        self.min_snr_db = min_snr_db
        self.max_snr_db = max_snr_db

        if not os.path.exists(noise_dir):
            raise IOError(f"Noise directory `{noise_dir}` does not exist")

        self.noise_files_list = list(pathlib.Path(noise_dir).glob("**/*.wav"))
        if len(self.noise_files_list) == 0:
            raise IOError(f"No .wav file found in the noise directory `{noise_dir}`")

    def __call__(self, data):
        audio_data, sample_rate = data

        random_noise_file = random.choice(self.noise_files_list)
        effects = [
            ["remix", "1"],
            ["rate", str(self.sample_rate)],
        ]
        noise, _ = torchaudio.sox_effects.apply_effects_file(
            random_noise_file, effects, normalize=True
        )
        audio_length = audio_data.shape[-1]
        noise_length = noise.shape[-1]

        if noise_length > audio_length:
            offset = random.randint(0, noise_length - audio_length)
            noise = noise[..., offset : offset + audio_length]

        elif noise_length < audio_length:
            noise = torch.cat(
                [noise, torch.zeros((noise.shape[0], audio_length - noise_length))],
                dim=-1,
            )

        snr_db = random.randint(self.min_snr_db, self.max_snr_db)
        snr = math.exp(snr_db / 10)
        audio_power = audio_data.norm(p=2)
        noise_power = noise.norm(p=2)
        scale = snr * noise_power / audio_power

        augmented_audio = (scale * audio_data + noise) / 2
        return augmented_audio, sample_rate


class ComposeTransform:
    def __init__(self, transforms):
        self.transforms = transforms

    def __call__(self, data):
        for t in self.transforms:
            data = t(data)
        return data


compose_transform = ComposeTransform(
    [
        ResampleAudio(target_sample_rate=TARGET_SAMPLE_RATE),
        RandomSpeedChange(sample_rate=TARGET_SAMPLE_RATE),
        RandomBackgroundNoise(sample_rate=TARGET_SAMPLE_RATE, noise_dir=NOISE_DIR),
    ]
)
