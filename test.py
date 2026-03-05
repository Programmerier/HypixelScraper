import requests
from decimal import Decimal, getcontext
import time

PRECISION = 1000         
PI_DIGITS  = 200         
N_VALUES   = [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000,100_000_000, 1_000_000_000]

getcontext().prec = PRECISION



def fetch_reference_pi(digits: int) -> str:
    """Holt Pi-Stellen von der kostenlosen pi.delivery API."""
    url = f"https://api.pi.delivery/v1/pi?start=0&numberOfDigits={digits + 1}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()["content"]  
    except Exception as e:
        print(f"  ⚠ API nicht erreichbar: {e}")
        # Fallback: erste 50 Stellen hart kodiert
        return "31415926535897932384626433832795028841971693993751"



def compute_pi(n):
    pi = Decimal(3)
    a  = 2
    for i in range(n):
        term = Decimal(4) / (a * (a + 1) * (a + 2))
        pi  += term if i % 2 == 0 else -term
        a   += 2
    return pi


def count_correct_digits(computed, reference_digits):
    
    ref = reference_digits 

    s = str(computed)     
    s = s.replace("3.", "3").replace(".", "")

    correct = 0
    for a, b in zip(s, ref):
        if a == b:
            correct += 1
        else:
            break

    return max(0, correct - 1)



def main():
    print("=" * 55)
    print("  Pi-Genauigkeitsmesser  (Nilakantha-Reihe)")
    print("=" * 55)

    print(f"\n→ Lade {PI_DIGITS} Pi-Stellen von api.pi.delivery …")
    ref = fetch_reference_pi(PI_DIGITS)
    pi_ref_display = "3." + ref[1:21] + "…"
    print(f"  Referenz-Pi : {pi_ref_display}\n")

    print(f"{'n':>10}  {'Zeit':>8}  {'Korrekte Stellen':>17}")
    print("-" * 42)

    for n in N_VALUES:
        t0 = time.perf_counter()
        pi_val = compute_pi(n)
        elapsed = time.perf_counter() - t0

        digits = count_correct_digits(pi_val, ref)

        print(f"{n:>10,}  {elapsed:>7.2f}s  {digits:>17} Stelle(n)")

    print("\nFertig!")


if __name__ == "__main__":
    main()