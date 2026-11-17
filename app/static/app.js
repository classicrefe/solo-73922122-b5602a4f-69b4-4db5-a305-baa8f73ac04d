// 删除测量列前的确认；表单其余部分由服务端渲染，版本号来自用户所见页面。
document.addEventListener("submit", (event) => {
  const form = event.target;
  if (!(form instanceof HTMLFormElement)) return;
  if (form.dataset.confirm) {
    if (!window.confirm(form.dataset.confirm)) {
      event.preventDefault();
    }
  }
});

// 上传失败后服务端会把编号回填；若本次提交失败（网络中断 / 浏览器未收到
// 响应），同样保留在输入框中的编号，便于用户直接重试同一编号。
const uploadForm = document.getElementById("upload-form");
const uploadIdStorageKey = "metlake.upload_id";
if (uploadForm) {
  const uploadId = uploadForm.querySelector('input[name="upload_id"]');
  if (uploadId) {
    // 刷新页面时也可能丢失服务端回填，使用 sessionStorage 兜底保留编号。
    if (!uploadId.value) {
      uploadId.value = sessionStorage.getItem(uploadIdStorageKey) || "";
    }
    uploadId.addEventListener("input", () => {
      sessionStorage.setItem(uploadIdStorageKey, uploadId.value);
    });
    uploadForm.addEventListener("submit", () => {
      sessionStorage.setItem(uploadIdStorageKey, uploadId.value);
    });
  }
} else if (location.pathname.startsWith("/batches/")) {
  // 到达批次页说明上一次上传已成功（首次或幂等重放），清掉暂存编号，
  // 避免之后的无关上传误用旧编号。
  sessionStorage.removeItem(uploadIdStorageKey);
}

// 批次表格在窄屏下允许横向滚动，不额外处理。
