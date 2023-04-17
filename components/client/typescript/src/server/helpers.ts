export function timeout(ms: number, abortController?: AbortController): Promise<void> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      resolve();
    }, ms);
    abortController?.signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timeout);
        reject(new Error(`Timeout aborted`));
      },
      { once: true }
    );
  });
}
