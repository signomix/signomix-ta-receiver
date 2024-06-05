
# `sgx0.addCommand(targetEUI, payload, overwrite)`

Funkcja `addCommand` służy do dodawania nowej komendy do obiektu `sgx0`.

## Składnia

```javascript
sgx0.addCommand(targetEUI, payload, overwrite)
```

### Parametry

- `targetEUI` (String): Unikalny identyfikator docelowego urządzenia.
- `payload` (Object): Dane w formacie JSON, które mają być wysłane jako część komendy.
- `overwrite` (Boolean): Flaga wskazująca, czy istniejąca komenda powinna zostać nadpisana.

### Zwraca

- `void`: Funkcja nie zwraca żadnej wartości.

## Przykład użycia

```javascript
const targetEUI = "00124B0004F12345";
const payload = {
    command: "activate",
    parameters: {
        duration: 10
    }
};
const overwrite = true;

sgx0.addCommand(targetEUI, payload, overwrite);
```