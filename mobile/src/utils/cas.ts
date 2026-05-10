const CAS_PATTERN = /^(\d{1,9})-(\d{2})-(\d)$/;

export function isValidCasFormat(cas: string) {
  return CAS_PATTERN.test(cas.trim());
}

export function normalizeCasInput(raw: string) {
  const input = raw.trim();
  if (!input) {
    return "";
  }

  for (const token of input.split(/\s+/)) {
    const cleaned = token.replace(/[(),]/g, "");
    if (CAS_PATTERN.test(cleaned)) {
      return cleaned;
    }
  }
  return input;
}

export function casChecksum(digits: string) {
  let total = 0;
  const n = digits.length;
  for (let index = 0; index < digits.length; index += 1) {
    total += Number(digits[index]) * (n - index);
  }
  return total % 10;
}

export function validateCas(cas: string) {
  const match = cas.trim().match(CAS_PATTERN);
  if (!match) {
    return { isValid: false, normalizedCas: cas.trim() };
  }

  const [, first, second, check] = match;
  const digits = `${first}${second}${check}`;
  return {
    isValid: String(casChecksum(digits)) === check,
    normalizedCas: `${first}-${second}-${check}`,
  };
}
