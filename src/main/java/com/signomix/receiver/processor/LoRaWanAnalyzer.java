package com.signomix.receiver.processor;

/**
 * Klasa do analizy parametrów transmisji LoRaWAN.
 */
public class LoRaWanAnalyzer {


    /**
     * Analizuje parametry transmisji i zwraca jakość połączenia oraz sugestię zmiany SF.
     *
     * @param currentSf Aktualnie używany Spreading Factor (7-12).
     * @param snr Stosunek sygnału do szumu (SNR) w dB.
     * @param rssi Siła odbieranego sygnału (RSSI) w dBm.
     * @return Obiekt LoRaWANAnalysisResult z obliczonymi wartościami.
     */
    public static LoRaWanAnalysisResult analyze(long currentSf, double snr, long rssi) {
        int quality;
        int possibleSfChange = 0; // Domyślnie: bez zmian

        // --- Krok 1: Oblicz jakość połączenia ---
        if (snr >= 10 && rssi > -85) {
            quality = 3; // Bardzo Dobre
        } else if (snr >= 5 && rssi > -100) {
            quality = 2; // Dobre
        } else if (snr >= -5 && rssi > -110) {
            quality = 1; // Słabe
        } else {
            quality = 0; // Złe
        }

        // --- Krok 2: Ustal sugestię zmiany SF na podstawie jakości ---
        if (quality == 3) {
            // Sygnał jest bardzo mocny, można spróbować zmniejszyć SF, aby oszczędzać energię i czas nadawania.
            if (currentSf > 7) {
                possibleSfChange = -1; // Sugeruj zmniejszenie SF
            }
        } else if (quality < 2) { // Jakość jest słaba (1) lub zła (0)
            // Sygnał jest słaby, należy go wzmocnić, zwiększając SF.
            if (currentSf < 12) {
                possibleSfChange = 1; // Sugeruj zwiększenie SF
            }
        }
        // Dla jakości "Dobrej" (2), possibleSfChange pozostaje 0 (bez zmian).

        return new LoRaWanAnalysisResult(quality, possibleSfChange);
    }

    /**
     * Metoda główna do demonstracji działania algorytmu.
     */
    /* public static void main(String[] args) {
        // Przykładowe dane wejściowe
        int sf = 7;
        double snr = 7.5;
        int rssi = -86;

        System.out.println("Analiza dla parametrów: SF=" + sf + ", SNR=" + snr + ", RSSI=" + rssi);

        // Wywołanie funkcji analizującej
        LoRaWanAnalysisResult result = analyze(sf, snr, rssi);

        // Wyświetlenie wyników
        System.out.println("--- Wynik Algorytmu ---");
        System.out.println("Jakość (quality): " + result.quality());
        System.out.println("Sugerowana zmiana SF (possible_SF_Change): " + result.possibleSfChange());

        // Interpretacja wyników
        String qualityDescription = switch (result.quality()) {
            case 0 -> "Złe 🔴";
            case 1 -> "Słabe 🟠";
            case 2 -> "Dobre 🟡";
            case 3 -> "Bardzo Dobre 🟢";
            default -> "Nieznane";
        };

        String sfChangeDescription = switch (result.possibleSfChange()) {
            case -1 -> "Zmniejsz SF (np. do SF" + (sf - 1) + ")";
            case 1 -> "Zwiększ SF (np. do SF" + (sf + 1) + ")";
            default -> "Bez zmian";
        };
        
        System.out.println("--- Interpretacja ---");
        System.out.println("Jakość połączenia: " + qualityDescription);
        System.out.println("Rekomendacja: " + sfChangeDescription);
    } */
}
