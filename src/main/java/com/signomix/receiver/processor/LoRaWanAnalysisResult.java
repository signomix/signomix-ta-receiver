package com.signomix.receiver.processor;

public class LoRaWanAnalysisResult{
        private final int quality;
        private final int possibleSfChange;

        public LoRaWanAnalysisResult(int quality, int possibleSfChange) {
            this.quality = quality;
            this.possibleSfChange = possibleSfChange;
        }

        public int quality() {
            return quality;
        }

        public int possibleSfChange() {
            return possibleSfChange;
        }
    }