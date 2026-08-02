// Web Crypto implementation for the embedded runtime.
//
// The common WebCrypto paths use typed-array host ops.
// Cells keeps a second, JSON-shaped op for Workers-compatible algorithms that
// require structured arguments (RSA-OAEP, ECDSA, and Ed25519).
(function () {
  const _randomValues = $$randomValues;
  const _digest = $$digest;
  const _hmacSign = $$hmacSign;
  const _hmacVerify = $$hmacVerify;
  const _aesEncrypt = $$aesEncrypt;
  const _aesDecrypt = $$aesDecrypt;

  const _DIGEST_ALGS = new Set([
    "SHA-1", "SHA-256", "SHA-384", "SHA-512",
  ]);
  const _SECRET_KEY_ALGS = new Set(["HMAC", "AES-GCM", "AES-CBC"]);

  function _toBuf(data) {
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) {
      return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    throw new TypeError("expected an ArrayBuffer or ArrayBufferView");
  }

  function _algorithmName(algorithm) {
    return String(
      typeof algorithm === "string" ? algorithm : algorithm?.name || "",
    ).toUpperCase();
  }

  function _hashName(hash) {
    return _algorithmName(
      typeof hash === "string" ? hash : hash?.name || "SHA-256",
    );
  }

  function _notSupported(message) {
    return new DOMException(message, "NotSupportedError");
  }

  function _operationError(message) {
    return new DOMException(message, "OperationError");
  }

  class CryptoKey {
    constructor(type, algorithm, extractable, usages, material) {
      Object.defineProperties(this, {
        type: { value: type, enumerable: true },
        algorithm: { value: algorithm, enumerable: true },
        extractable: { value: Boolean(extractable), enumerable: true },
        usages: { value: Object.freeze(Array.from(usages || [])), enumerable: true },
        __celldMaterial: { value: material },
      });
    }
    get [Symbol.toStringTag]() { return "CryptoKey"; }
  }

  function _makeKey(type, algorithm, extractable, usages, material) {
    return new CryptoKey(type, algorithm, extractable, usages, material);
  }

  function _extra(operation, input) {
    return JSON.parse(__crypto_operation(operation, JSON.stringify(input)));
  }

  class SubtleCrypto {
    get [Symbol.toStringTag]() { return "SubtleCrypto"; }

    async digest(algorithm, data) {
      const name = _algorithmName(algorithm);
      if (!_DIGEST_ALGS.has(name)) {
        throw _notSupported("unsupported digest algorithm: " + name);
      }
      const out = _digest(name, _toBuf(data));
      return out.buffer.slice(out.byteOffset, out.byteOffset + out.byteLength);
    }

    async importKey(format, keyData, algorithm, extractable, usages) {
      const name = _algorithmName(algorithm);
      if (format === "raw" && _SECRET_KEY_ALGS.has(name)) {
        const raw = _toBuf(keyData).slice();
        const normalized = name === "HMAC"
          ? { name, hash: { name: _hashName(algorithm?.hash) }, length: raw.byteLength * 8 }
          : { name, length: raw.byteLength * 8 };
        return _makeKey(
          "secret", normalized, extractable, usages, { bytes: raw },
        );
      }
      if (format === "pkcs8" && (name === "ED25519" || name === "ECDSA")) {
        return _makeKey(
          "private", algorithm, extractable, usages,
          { bytes: _toBuf(keyData).slice() },
        );
      }
      if (format === "jwk" && name === "RSA-OAEP") {
        return _makeKey(
          keyData?.d ? "private" : "public",
          algorithm,
          extractable,
          usages,
          { jwk: structuredClone(keyData) },
        );
      }
      throw _notSupported("unsupported key import");
    }

    async exportKey(format, key) {
      if (format === "raw" && key?.__celldMaterial?.bytes) {
        if (!key.extractable) {
          throw new DOMException("key is not extractable", "InvalidAccessError");
        }
        const raw = key.__celldMaterial.bytes;
        return raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength);
      }
      if (format === "jwk" && key?.__celldMaterial?.jwk) {
        if (!key.extractable) {
          throw new DOMException("key is not extractable", "InvalidAccessError");
        }
        return structuredClone(key.__celldMaterial.jwk);
      }
      throw _notSupported("unsupported key export");
    }

    async generateKey(algorithm, extractable, usages) {
      const name = _algorithmName(algorithm);
      if (_SECRET_KEY_ALGS.has(name)) {
        let byteLength;
        if (name === "HMAC") {
          const defaults = {
            "SHA-1": 20,
            "SHA-256": 32,
            "SHA-384": 48,
            "SHA-512": 64,
          };
          const hash = _hashName(algorithm?.hash);
          byteLength = algorithm?.length
            ? Number(algorithm.length) / 8
            : defaults[hash];
          if (!byteLength) throw _notSupported("unsupported HMAC hash: " + hash);
          const raw = new Uint8Array(byteLength);
          crypto.getRandomValues(raw);
          return _makeKey(
            "secret",
            { name, hash: { name: hash }, length: raw.byteLength * 8 },
            extractable,
            usages,
            { bytes: raw },
          );
        }
        byteLength = Number(algorithm?.length || 256) / 8;
        if (byteLength !== 16 && byteLength !== 32) {
          throw new DOMException("AES-GCM length must be 128 or 256", "OperationError");
        }
        const raw = new Uint8Array(byteLength);
        crypto.getRandomValues(raw);
        return _makeKey(
          "secret",
          { name, length: raw.byteLength * 8 },
          extractable,
          usages,
          { bytes: raw },
        );
      }
      if (name === "RSA-OAEP") {
        const pair = _extra("rsa-generate", {});
        return {
          publicKey: _makeKey(
            "public", algorithm, true,
            usages.filter((usage) => usage === "encrypt"),
            { jwk: pair.publicKey },
          ),
          privateKey: _makeKey(
            "private", algorithm, extractable,
            usages.filter((usage) => usage === "decrypt"),
            { jwk: pair.privateKey },
          ),
        };
      }
      throw _notSupported("unsupported key algorithm: " + name);
    }

    async sign(algorithm, key, data) {
      const name = _algorithmName(algorithm || key?.algorithm);
      const bytes = _toBuf(data);
      if (name === "HMAC") {
        const hash = _hashName(key?.algorithm?.hash);
        const sig = _hmacSign(hash, key.__celldMaterial.bytes, bytes);
        if (!sig) throw _operationError("HMAC sign failed");
        return sig.buffer.slice(sig.byteOffset, sig.byteOffset + sig.byteLength);
      }
      const operation = name === "ED25519"
        ? "ed25519-sign"
        : name === "ECDSA"
          ? "p256-sign"
          : null;
      if (!operation) throw _notSupported("unsupported sign algorithm: " + name);
      const result = _extra(operation, {
        key: Array.from(key?.__celldMaterial?.bytes || []),
        data: Array.from(bytes),
      });
      return Uint8Array.from(result.bytes).buffer;
    }

    async verify(algorithm, key, signature, data) {
      const name = _algorithmName(algorithm || key?.algorithm);
      if (name !== "HMAC") {
        throw _notSupported("unsupported verify algorithm: " + name);
      }
      return _hmacVerify(
        _hashName(key?.algorithm?.hash),
        key.__celldMaterial.bytes,
        _toBuf(signature),
        _toBuf(data),
      );
    }

    async encrypt(algorithm, key, data) {
      const name = _algorithmName(algorithm);
      if (name !== "AES-GCM") {
        throw _notSupported("unsupported encrypt algorithm: " + name);
      }
      const out = _aesEncrypt(
        key.__celldMaterial.bytes,
        _toBuf(algorithm.iv),
        _toBuf(data),
      );
      if (!out) throw _operationError("AES-GCM encrypt failed");
      return out.buffer.slice(out.byteOffset, out.byteOffset + out.byteLength);
    }

    async decrypt(algorithm, key, data) {
      const name = _algorithmName(algorithm);
      if (name === "AES-GCM") {
        const out = _aesDecrypt(
          key.__celldMaterial.bytes,
          _toBuf(algorithm.iv),
          _toBuf(data),
        );
        if (!out) throw _operationError("AES-GCM decrypt failed");
        return out.buffer.slice(out.byteOffset, out.byteOffset + out.byteLength);
      }
      if (name === "RSA-OAEP") {
        const result = _extra("rsa-oaep-decrypt", {
          jwk: key?.__celldMaterial?.jwk,
          data: Array.from(_toBuf(data)),
        });
        return Uint8Array.from(result.bytes).buffer;
      }
      throw _notSupported("unsupported decrypt algorithm: " + name);
    }
  }

  const subtle = new SubtleCrypto();
  const crypto = {
    getRandomValues(array) {
      // Web IDL brand check, observable via node:crypto's webcrypto alias.
      if (this !== crypto) throw new TypeError("Illegal invocation");
      if (
        !(array instanceof Int8Array) &&
        !(array instanceof Uint8Array) &&
        !(array instanceof Uint8ClampedArray) &&
        !(array instanceof Int16Array) &&
        !(array instanceof Uint16Array) &&
        !(array instanceof Int32Array) &&
        !(array instanceof Uint32Array) &&
        !(array instanceof BigInt64Array) &&
        !(array instanceof BigUint64Array)
      ) {
        throw new DOMException(
          "Argument is not an integer-typed array",
          "TypeMismatchError",
        );
      }
      if (array.byteLength > 65536) {
        throw new DOMException(
          "getRandomValues byteLength must be at most 65536",
          "QuotaExceededError",
        );
      }
      _randomValues(array);
      return array;
    },

    randomUUID() {
      const bytes = new Uint8Array(16);
      _randomValues(bytes);
      bytes[6] = (bytes[6] & 0x0f) | 0x40;
      bytes[8] = (bytes[8] & 0x3f) | 0x80;
      const h = (index) => bytes[index].toString(16).padStart(2, "0");
      return h(0) + h(1) + h(2) + h(3) + "-" +
        h(4) + h(5) + "-" + h(6) + h(7) + "-" +
        h(8) + h(9) + "-" + h(10) + h(11) +
        h(12) + h(13) + h(14) + h(15);
    },

    subtle,
    get [Symbol.toStringTag]() { return "Crypto"; },
  };

  globalThis.CryptoKey = CryptoKey;
  globalThis.SubtleCrypto = SubtleCrypto;
  globalThis.crypto = crypto;
})();

// Last harness script, so this sees every internal the others declared.
// Runtime plumbing must not show up in `for (const k in globalThis)`: a
// bundle walking the globals should find the Web platform and nothing
// else. Host ops are already non-enumerable; these are the JS-side ones.
for (const n of Object.getOwnPropertyNames(globalThis))
  if (n.startsWith("__") || n.startsWith("$$"))
    // A top-level `function` declaration is non-configurable, so a
    // couple of harness helpers cannot be hidden. Harmless: a walker
    // sees a function either way.
    try { Object.defineProperty(globalThis, n, { enumerable: false }); }
    catch { /* non-configurable */ }
