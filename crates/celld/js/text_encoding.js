// TextEncoder + TextDecoder (WHATWG Encoding).
// Extracted module from the bootstrap.js IIFE.
//
// TextEncoder always emits UTF-8 (per spec). TextDecoder
// decodes utf-8 / utf-16le / utf-16be in pure JS — the
// hot path, no host ops. Every other WHATWG label
// (windows-1252, big5, gbk/gb18030, iso-2022-jp,
// x-user-defined, …) is the slow path: encoding_rs on
// the host via the $$textDecoder* ops, one call per
// decode() with native streaming state per decoder.
(function () {
    const _tdLabel = $$textDecoderLabel;
    const _tdNew = $$textDecoderNew;
    const _tdDecode = $$textDecoderDecode;
    const _tdFree = $$textDecoderFree;
    // Frees native decoders abandoned mid-stream. Ids
    // are never reused, so a late free is a no-op.
    // Created on first legacy stream, not at boot.
    let _tdRegistry;
    const _tdTrack = (target, id) => {
        if (_tdRegistry === undefined) {
            _tdRegistry =
                typeof FinalizationRegistry === 'function'
                    ? new FinalizationRegistry(_tdFree)
                    : null;
        }
        _tdRegistry?.register(target, id);
    };
    globalThis.TextEncoder = class TextEncoder {
        get encoding() { return 'utf-8'; }
        encode(s = '') {
            s = String(s);
            const buf = new Uint8Array(s.length * 4);
            let w = 0;
            for (let i = 0; i < s.length; ) {
                let c = s.charCodeAt(i);
                if (c >= 0xD800 && c <= 0xDBFF &&
                    i + 1 < s.length) {
                    const trail = s.charCodeAt(i + 1);
                    if (trail >= 0xDC00 &&
                        trail <= 0xDFFF) {
                        c = ((c - 0xD800) << 10) +
                            (trail - 0xDC00) + 0x10000;
                    } else { c = 0xFFFD; }
                } else if (c >= 0xD800 && c <= 0xDFFF) {
                    c = 0xFFFD;
                }
                if (c < 0x80) {
                    buf[w++] = c;
                } else if (c < 0x800) {
                    buf[w++] = 0xc0 | (c >> 6);
                    buf[w++] = 0x80 | (c & 0x3f);
                } else if (c < 0x10000) {
                    buf[w++] = 0xe0 | (c >> 12);
                    buf[w++] = 0x80 | ((c>>6) & 0x3f);
                    buf[w++] = 0x80 | (c & 0x3f);
                } else {
                    buf[w++] = 0xf0 | (c >> 18);
                    buf[w++] = 0x80|((c>>12) & 0x3f);
                    buf[w++] = 0x80|((c>>6) & 0x3f);
                    buf[w++] = 0x80 | (c & 0x3f);
                }
                i += c > 0xffff ? 2 : 1;
            }
            // `slice`, not `subarray`: the scratch buffer is 4x the input
            // length, and a subarray would hand the caller a view onto it.
            // `encode(s).buffer` is a common idiom -- into crypto.subtle,
            // into a Blob -- and with a shared buffer it carries up to 3x
            // the bytes of trailing garbage.
            return buf.slice(0, w);
        }
        encodeInto(source, destination) {
            if (!(destination instanceof Uint8Array))
                throw new TypeError(
                    'encodeInto requires Uint8Array');
            source = String(source);
            let read = 0, written = 0;
            for (let i = 0; i < source.length; ) {
                let c = source.charCodeAt(i);
                // Handle surrogates
                if (c >= 0xD800 && c <= 0xDBFF &&
                    i + 1 < source.length) {
                    const trail =
                        source.charCodeAt(i + 1);
                    if (trail >= 0xDC00 &&
                        trail <= 0xDFFF) {
                        c = ((c - 0xD800) << 10) +
                            (trail - 0xDC00) + 0x10000;
                    } else {
                        c = 0xFFFD; // lone surrogate
                    }
                } else if (c >= 0xD800 && c <= 0xDFFF) {
                    c = 0xFFFD; // lone surrogate
                }
                let bytes;
                if (c < 0x80) bytes = 1;
                else if (c < 0x800) bytes = 2;
                else if (c < 0x10000) bytes = 3;
                else bytes = 4;
                if (written + bytes > destination.length)
                    break;
                if (bytes === 1) {
                    destination[written++] = c;
                } else if (bytes === 2) {
                    destination[written++] =
                        0xc0 | (c >> 6);
                    destination[written++] =
                        0x80 | (c & 0x3f);
                } else if (bytes === 3) {
                    destination[written++] =
                        0xe0 | (c >> 12);
                    destination[written++] =
                        0x80 | ((c >> 6) & 0x3f);
                    destination[written++] =
                        0x80 | (c & 0x3f);
                } else {
                    destination[written++] =
                        0xf0 | (c >> 18);
                    destination[written++] =
                        0x80 | ((c >> 12) & 0x3f);
                    destination[written++] =
                        0x80 | ((c >> 6) & 0x3f);
                    destination[written++] =
                        0x80 | (c & 0x3f);
                }
                i += c > 0xffff ? 2 : 1;
                read = i;
            }
            return { read, written };
        }
    };
    globalThis.TextDecoder = class TextDecoder {
        #encoding;
        #fatal;
        #ignoreBOM;
        #pending = [];
        #bomSeen = false;
        // Live native decoder id while a legacy-encoding
        // stream is in flight; undefined otherwise.
        #legacyId;
        constructor(label = 'utf-8', options = {}) {
            // Per WHATWG encoding: strip ASCII whitespace
            // only (not Unicode — \v, NBSP, etc. stay).
            label = String(label)
                .replace(/^[\t\n\f\r ]+|[\t\n\f\r ]+$/g, '')
                .toLowerCase();
            // The full WHATWG label set for the pure-JS encodings. A miss
            // here (e.g. 'ansi_x3.4-1968', which the standard maps to
            // windows-1252 rather than utf-8) resolves through the host's
            // encoding_rs label table instead.
            const aliases = {
                'utf-8': 'utf-8', 'utf8': 'utf-8',
                'unicode-1-1-utf-8': 'utf-8',
                'unicode11utf8': 'utf-8',
                'unicode20utf8': 'utf-8',
                'x-unicode20utf8': 'utf-8',
                'utf-16le': 'utf-16le',
                'utf-16': 'utf-16le',
                'ucs-2': 'utf-16le',
                'unicode': 'utf-16le',
                'unicodefeff': 'utf-16le',
                'iso-10646-ucs-2': 'utf-16le',
                'csunicode': 'utf-16le',
                'utf-16be': 'utf-16be',
                'unicodefffe': 'utf-16be',
            };
            let enc = aliases[label];
            if (!enc) {
                // undefined covers unknown labels and the replacement
                // encoding — both RangeError per spec. A resolved name
                // is canonical lowercase; route utf names (all already
                // aliases keys) back to the pure-JS path.
                const name = _tdLabel(label);
                if (name === undefined) throw new RangeError(
                    `The encoding label` +
                    ` '${label}' is invalid.`
                );
                enc = aliases[name] ?? name;
            }
            this.#encoding = enc;
            this.#fatal = !!options.fatal;
            this.#ignoreBOM = !!options.ignoreBOM;
        }
        get encoding() { return this.#encoding; }
        get fatal() { return this.#fatal; }
        get ignoreBOM() { return this.#ignoreBOM; }
        decode(input, options = {}) {
            const stream = !!options.stream;
            let b;
            if (input == null) {
                b = new Uint8Array(0);
            } else if (input instanceof ArrayBuffer) {
                b = new Uint8Array(input);
            } else if (ArrayBuffer.isView(input)) {
                // BufferSource (TypedArray / DataView).
                // Slice the underlying buffer at the
                // view's offset / length so we honour
                // sub-views.
                b = new Uint8Array(
                    input.buffer,
                    input.byteOffset,
                    input.byteLength,
                );
            } else {
                // Per Web IDL: `decode(input)` accepts
                // [AllowShared] BufferSource. Anything
                // else is a type-coercion error. Pre-fix
                // the fallback `new Uint8Array(input, 0,
                // input.length)` interpreted a number as
                // a length-N allocation of zero bytes —
                // `.decode(42)` returned 42 NUL chars,
                // `.decode("hello")` returned "" (no
                // .length on a string maps to a 0-length
                // buffer view), both spec-violating.
                throw new TypeError(
                    "TextDecoder.decode: input must be"
                    + " a BufferSource",
                );
            }
            // Prepend any pending bytes.
            if (this.#pending.length > 0) {
                const merged = new Uint8Array(
                    this.#pending.length + b.length);
                merged.set(this.#pending);
                merged.set(b, this.#pending.length);
                b = merged;
                this.#pending = [];
            }
            if (this.#encoding === 'utf-8') {
                return this._decode8(b, stream);
            }
            if (this.#encoding === 'utf-16le' ||
                this.#encoding === 'utf-16be') {
                return this._decode16(b, stream);
            }
            return this._decodeLegacy(b, stream);
        }
        // Legacy WHATWG encodings: one host op per decode(). Streaming
        // state (multibyte carry-over, ISO-2022-JP mode) lives in the
        // native decoder; a fatal error frees it, matching Workerd —
        // the next decode starts clean.
        _decodeLegacy(b, stream) {
            let id = this.#legacyId;
            if (id === undefined) {
                id = _tdNew(this.#encoding, this.#ignoreBOM);
                if (stream) {
                    this.#legacyId = id;
                    _tdTrack(this, id);
                }
            } else if (!stream) {
                this.#legacyId = undefined;
            }
            try {
                return _tdDecode(id, b, this.#fatal, !stream);
            } catch (e) {
                this.#legacyId = undefined;
                throw e;
            }
        }
        _decode8(b, stream) {
            const fatal = this.#fatal;
            const fail = () => {
                if (fatal) throw new TypeError(
                    'The encoded data is not valid.');
                return '�';
            };
            const isCont = (x) =>
                x !== undefined && (x & 0xC0) === 0x80;
            // Expected byte count for lead byte.
            const seqLen = (c) => {
                if (c < 0x80) return 1;
                if ((c & 0xE0) === 0xC0) return 2;
                if ((c & 0xF0) === 0xE0) return 3;
                if ((c & 0xF8) === 0xF0) return 4;
                return 0; // invalid
            };
            let s = '', i = 0;
            if (!this.#bomSeen && !this.#ignoreBOM &&
                b.length >= 3 && b[0]===0xEF &&
                b[1]===0xBB && b[2]===0xBF) {
                i = 3;
                this.#bomSeen = true;
            }
            while (i < b.length) {
                const c0 = b[i];
                const need = seqLen(c0);
                if (need === 0) {
                    s += fail(); i++; continue;
                }
                if (need === 1) {
                    s += String.fromCharCode(c0);
                    i++; continue;
                }
                // Early reject invalid lead bytes.
                if (need === 2 && c0 < 0xC2)
                    { s += fail(); i++; continue; }
                if (need === 4 && c0 > 0xF4)
                    { s += fail(); i++; continue; }
                // Find how many valid cont bytes follow.
                let valid = 0;
                for (let j = 1; j < need &&
                     i + j < b.length; j++) {
                    if (!isCont(b[i+j])) break;
                    valid++;
                }
                const have = 1 + valid;
                // Range checks on partial data.
                let rangeOk = true;
                if (valid >= 1 && need >= 3) {
                    const b1 = b[i+1];
                    if (need===3 && c0===0xE0 && b1<0xA0)
                        rangeOk = false;
                    if (need===3 && c0===0xED && b1>=0xA0)
                        rangeOk = false;
                    if (need===4 && c0===0xF0 && b1<0x90)
                        rangeOk = false;
                    if (need===4 && c0===0xF4 && b1>=0x90)
                        rangeOk = false;
                }
                if (!rangeOk) {
                    s += fail(); i++; continue;
                }
                if (have < need) {
                    // Buffer only if we're at end of
                    // input and all bytes so far are
                    // valid continuations.
                    if (stream &&
                        i + have === b.length) {
                        this.#pending =
                            Array.from(b.slice(i));
                        break;
                    }
                    // Emit one FFFD, skip past valid
                    // continuation bytes.
                    s += fail();
                    i += have;
                    continue;
                }
                if (need === 2) {
                    s += String.fromCharCode(
                        ((c0&0x1F)<<6)
                        |(b[i+1]&0x3F));
                    i += 2;
                } else if (need === 3) {
                    const cp = ((c0&0x0F)<<12)
                        |((b[i+1]&0x3F)<<6)
                        |(b[i+2]&0x3F);
                    if (cp < 0x800 ||
                        (cp>=0xD800 && cp<=0xDFFF))
                        { s += fail(); i++; continue; }
                    s += String.fromCharCode(cp);
                    i += 3;
                } else {
                    const cp = ((c0&0x07)<<18)
                        |((b[i+1]&0x3F)<<12)
                        |((b[i+2]&0x3F)<<6)
                        |(b[i+3]&0x3F);
                    if (cp < 0x10000 || cp > 0x10FFFF)
                        { s += fail(); i++; continue; }
                    s += String.fromCodePoint(cp);
                    i += 4;
                }
            }
            // A non-stream decode ends the sequence: the
            // next call starts a fresh decoder, so BOM
            // stripping applies again (spec step 1).
            if (!stream) {
                this.#pending = [];
                this.#bomSeen = false;
            }
            return s;
        }
        _decode16(b, stream) {
            const be = this.#encoding === 'utf-16be';
            const fatal = this.#fatal;
            const fail = () => {
                if (fatal) throw new TypeError(
                    'The encoded data is not valid.');
                return '�';
            };
            let s = '', i = 0;
            if (!this.#bomSeen && !this.#ignoreBOM &&
                b.length >= 2) {
                if (b[0]===0xFF && b[1]===0xFE && !be) {
                    i = 2; this.#bomSeen = true;
                } else if (b[0]===0xFE &&
                           b[1]===0xFF && be) {
                    i = 2; this.#bomSeen = true;
                }
            }
            while (i + 1 < b.length) {
                const unitStart = i;
                const lo = be ? b[i+1] : b[i];
                const hi = be ? b[i] : b[i+1];
                const code = lo | (hi << 8);
                i += 2;
                if (code >= 0xD800 && code <= 0xDBFF) {
                    // Lead surrogate — look for trail.
                    if (i + 1 < b.length) {
                        const lo2 = be ? b[i+1] : b[i];
                        const hi2 = be ? b[i] : b[i+1];
                        const trail = lo2 | (hi2 << 8);
                        if (trail >= 0xDC00
                            && trail <= 0xDFFF) {
                            const cp =
                                ((code - 0xD800) << 10)
                                + (trail - 0xDC00)
                                + 0x10000;
                            i += 2;
                            s += String.fromCodePoint(cp);
                            continue;
                        }
                        s += fail();
                        continue;
                    }
                    if (stream) {
                        i = unitStart;
                        break;
                    }
                    s += fail();
                } else if (code >= 0xDC00
                           && code <= 0xDFFF) {
                    s += fail();
                } else {
                    s += String.fromCharCode(code);
                }
            }
            if (i < b.length) {
                if (stream) {
                    this.#pending =
                        Array.from(b.slice(i));
                } else if (this.#fatal) {
                    throw new TypeError(
                        'The encoded data is not valid.'
                    );
                }
            }
            if (!stream) this.#bomSeen = false;
            return s;
        }
    };
    // Web IDL toStringTag.
    Object.defineProperty(globalThis.TextEncoder.prototype,
      Symbol.toStringTag,
      { value: 'TextEncoder', configurable: true });
    Object.defineProperty(globalThis.TextDecoder.prototype,
      Symbol.toStringTag,
      { value: 'TextDecoder', configurable: true });
})();
