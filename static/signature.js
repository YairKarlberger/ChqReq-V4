document.addEventListener('DOMContentLoaded', () => {
  const canvas = document.getElementById('signature-canvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const hiddenInput = document.getElementById('signature_data');
  let drawing = false;
  let lastPos = null;

  const resize = () => {
    const data = canvas.toDataURL();
    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;
    if (data) {
      const image = new Image();
      image.onload = () => ctx.drawImage(image, 0, 0, canvas.width, canvas.height);
      image.src = data;
    }
  };

  const pos = (e) => {
    if (e.touches && e.touches.length) {
      const rect = canvas.getBoundingClientRect();
      return {
        x: e.touches[0].clientX - rect.left,
        y: e.touches[0].clientY - rect.top,
      };
    }
    const rect = canvas.getBoundingClientRect();
    return {
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
    };
  };

  const start = (e) => {
    drawing = true;
    lastPos = pos(e);
  };

  const draw = (e) => {
    if (!drawing) return;
    e.preventDefault();
    const current = pos(e);
    ctx.beginPath();
    ctx.moveTo(lastPos.x, lastPos.y);
    ctx.lineTo(current.x, current.y);
    ctx.strokeStyle = '#1e293b';
    ctx.lineWidth = 2;
    ctx.lineCap = 'round';
    ctx.stroke();
    lastPos = current;
    hiddenInput.value = canvas.toDataURL('image/png');
  };

  const stop = () => {
    drawing = false;
  };

  const clearBtn = document.getElementById('clear-signature');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      hiddenInput.value = '';
    });
  }

  window.addEventListener('resize', resize);
  canvas.addEventListener('mousedown', start);
  canvas.addEventListener('touchstart', start);
  canvas.addEventListener('mousemove', draw);
  canvas.addEventListener('touchmove', draw);
  canvas.addEventListener('mouseup', stop);
  canvas.addEventListener('mouseleave', stop);
  canvas.addEventListener('touchend', stop);

  resize();
});
