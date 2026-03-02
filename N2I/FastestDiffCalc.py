from dataclasses import dataclass
from coincurve import PrivateKey
import hashlib, hmac, struct

def sha256(b: bytes) -> bytes:
    return hashlib.sha256(b).digest()

def sha256d(b: bytes) -> bytes:
    return sha256(sha256(b))

def ser_compact_size(n: int) -> bytes:
    if n < 253:
        return bytes([n])
    elif n < 0x10000:
        return b"\xfd" + struct.pack("<H", n)
    elif n < 0x100000000:
        return b"\xfe" + struct.pack("<I", n)
    else:
        return b"\xff" + struct.pack("<Q", n)

P  = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
N  = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
Gx = 55066263022277343669578718895168534326250603453777594175500187360389116729240
Gy = 32670510020758816978083085130507043184471273380659243275938904335757337482424
G  = (Gx, Gy)

def nonce_function_rfc6979(privkey32: bytes, msg32: bytes, ndata: bytes = b"", algo16: bytes = b"") -> int:
    assert len(privkey32) == 32
    assert len(msg32) == 32
    assert len(algo16) in (0, 16)
    assert len(ndata) in (0, 32)

    V = b"\x01" * 32
    K = b"\x00" * 32
    blob = privkey32 + msg32 + ndata + algo16

    K = hmac.new(K, V + b"\x00" + blob, hashlib.sha256).digest()
    V = hmac.new(K, V, hashlib.sha256).digest()
    K = hmac.new(K, V + b"\x01" + blob, hashlib.sha256).digest()
    V = hmac.new(K, V, hashlib.sha256).digest()

    while True:
        V = hmac.new(K, V, hashlib.sha256).digest()
        k = int.from_bytes(V, "big")
        if 0 < k < N:
            return k
        K = hmac.new(K, V + b"\x00", hashlib.sha256).digest()
        V = hmac.new(K, V, hashlib.sha256).digest()

def jacobi_is_minus_one(a: int) -> bool:
    a %= P
    if a == 0:
        return False
    t = pow(a, (P - 1) >> 1, P)
    return t == P - 1

def point_mul_G(k: int):
    k %= N
    if k == 0:
        return None
    kb = k.to_bytes(32, "big")
    pub65 = PrivateKey(kb).public_key.format(compressed=False)
    x = int.from_bytes(pub65[1:33], "big")
    y = int.from_bytes(pub65[33:65], "big")
    return (x, y)

def schnorr_sign_2019(privkey32: bytes, msg32: bytes) -> bytes:
    assert len(privkey32) == 32
    assert len(msg32) == 32

    x = int.from_bytes(privkey32, "big")
    if x == 0 or x >= N:
        raise ValueError("invalid privkey")

    k = nonce_function_rfc6979(privkey32, msg32, algo16=b"Schnorr+SHA256  ")

    R = point_mul_G(k)
    Ppt = point_mul_G(x)
    rx, ry = R   # type: ignore
    px, py = Ppt # type: ignore

    if jacobi_is_minus_one(ry):
        k = N - k
        ry = (P - ry) % P

    rbytes = rx.to_bytes(32, "big")
    pubkey33 = (b"\x02" if (py % 2 == 0) else b"\x03") + px.to_bytes(32, "big")

    e = int.from_bytes(sha256(rbytes + pubkey33 + msg32), "big")
    s = (k + e * x) % N
    return rbytes + s.to_bytes(32, "big")

def nexa_powhash(header_commitment_hex: str, solution_nonce_hex: str) -> bytes:
    header = bytes.fromhex(header_commitment_hex)
    nonce  = bytes.fromhex(solution_nonce_hex)
    payload = header + ser_compact_size(len(nonce)) + nonce

    mining_hash = sha256d(payload)
    h1 = sha256(mining_hash)
    sig = schnorr_sign_2019(mining_hash, h1)
    final_hash = sha256(sig)
    return final_hash

DIFF1_TARGET = 0xFFFF * (1 << 208)

def pdiff_from_powhash(final_hash32: bytes) -> float:
    hv = int.from_bytes(final_hash32, "little")
    return DIFF1_TARGET / hv

def target_from_setdiff(set_diff: float) -> int:
    return int(DIFF1_TARGET / set_diff)

def check_share(header_commitment_hex: str, extranonce1_hex_8B: str, nonce8_hex_8B: str, set_diff: float):
    solution_nonce = extranonce1_hex_8B + nonce8_hex_8B
    fh = nexa_powhash(header_commitment_hex, solution_nonce)
    d = pdiff_from_powhash(fh)
    hv = int.from_bytes(fh, "little")
    ok = hv <= target_from_setdiff(set_diff)
    return d, ok, fh[::-1].hex()

def diff_calc(header, solution):
    return pdiff_from_powhash(nexa_powhash(header, solution))

def diff_M(diff):
    diff *= 2**32
    diff /= 1000
    diff /= 1000
    return diff

def diff_show(diff):
    diff = diff_M(diff)
    if diff < 1000:
        return f"{diff:.0f}M"
    else:
        diff /= 1000
        return f"{diff:.2f}G"

if __name__ == "__main__":
    extranonce1 = "1a93000d00000000"
    set_diff = 0.6984812728205725

    jobs = {
        "d6671d87": "d6671d87ba27d4b5a679009b7568d42a7b1486d0cfe28fd6310b9cbbb71a75bb",
        "477775d6": "477775d6be14f5cd75ab5a3a978df46d6c4777fb70daa8492448b4b06ef423b8",
        'a98d3cec': 'a98d3cec935487e7c1eff6b85ce7bdab55bf121110c1d99a9a6383146a28d00d'
    }
    submits = [
        ("d6671d87", "3fe11eff7b296553"),
        ("d6671d87", "8083cd0c7c296553"),
        ("477775d6", "c98c01f370c69470"),
        ("477775d6", "eb0c4b0671c69470"),
    ]

    for job_id, nonce8 in submits:
        d, ok, powhash_hex = check_share(jobs[job_id], extranonce1, nonce8, set_diff)
        print(job_id, nonce8, "pdiff", d, "ok", ok, "powhash", powhash_hex)

    import time
    print("start")
    start = time.time()
    for i in range(100):
        diff_calc("a98d3cec935487e7c1eff6b85ce7bdab55bf121110c1d99a9a6383146a28d00d", "d8f3a7350000010004f8000d00000000")
    time_ms = (time.time() -start) *1000
    print(f"end {time_ms:.2f}ms")
