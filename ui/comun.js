// Funciones compartidas por todas las pantallas de WaterManagement (sin librerías).
// Cada página define pintar(datos) y llama a sondear('/api/estado', pintar, 'nombre').

const $ = sel => document.querySelector(sel);

// Estados de las WS (nombres EXACTOS de WM_Central.py) -> clase CSS (comun.css) y texto
const CLASE = {DISPONIBLE: 'disponible', REGANDO: 'regando', FUGA: 'fuga', FUERA_SERVICIO: 'fuera', DESCONECTADA: 'desconectada'};
const ETIQUETA = {DISPONIBLE: 'Disponible', REGANDO: 'Regando', FUGA: 'Fuga', FUERA_SERVICIO: 'Fuera de servicio', DESCONECTADA: 'Desconectada'};

// Escapar texto que viene de la red antes de meterlo en innerHTML
function esc(x) {
  return String(x ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
}

// 12.345 -> "12,3" (formato español)
function num(x, decimales = 1) {
  return Number(x || 0).toLocaleString('es-ES', {minimumFractionDigits: decimales, maximumFractionDigits: decimales});
}

// epoch (s) -> "14:03:27"
function hora(t) {
  return t ? new Date(t * 1000).toLocaleTimeString('es-ES', {hour12: false}) : '–';
}

// segundos -> "1 h 05 min" / "3 min 12 s" / "8 s"
function duracion(s) {
  s = Math.max(0, Math.floor(s || 0));
  const h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60), seg = s % 60;
  if (h) return `${h} h ${String(m).padStart(2, '0')} min`;
  if (m) return `${m} min ${seg} s`;
  return `${seg} s`;
}

// "hace 5 s" a partir de un epoch y el "ahora" del servidor (no el del navegador)
function hace(t, ahora) {
  return t ? `hace ${duracion(ahora - t)}` : 'nunca';
}

// innerHTML solo si cambia: no se reinician animaciones ni se pierde la selección de texto
function fijarHtml(el, html) {
  if (el && el.dataset.html !== html) { el.dataset.html = html; el.innerHTML = html; }
}

// Lista <ol> de eventos {t, nivel, texto}; el más nuevo primero
function pintarEventos(ol, eventos) {
  fijarHtml(ol, (eventos || []).map(e =>
    `<li class="${esc(e.nivel)}"><time>${hora(e.t)}</time><span>${esc(e.texto)}</span></li>`).join('')
    || '<li><time></time><span class="tenue">Sin eventos todavía.</span></li>');
}

// Banda roja arriba cuando el proceso deja de responder
function bandaDesconexion(visible, quien) {
  let b = document.getElementById('banda-desconexion');
  if (!b) {
    b = document.createElement('div');
    b.id = 'banda-desconexion'; b.className = 'desconexion'; b.setAttribute('role', 'alert');
    document.body.prepend(b);
  }
  b.hidden = !visible;
  b.textContent = `Sin respuesta de ${quien}. Se reintenta cada segundo…`;
}

// Aviso flotante abajo (desaparece solo)
let temporizadorAviso = null;
function avisar(texto, error = false) {
  let a = document.getElementById('aviso');
  if (!a) {
    a = document.createElement('div');
    a.id = 'aviso'; a.setAttribute('role', 'status');
    document.body.append(a);
  }
  a.className = 'aviso' + (error ? ' error' : '');
  a.textContent = texto; a.hidden = false;
  clearTimeout(temporizadorAviso);
  temporizadorAviso = setTimeout(() => { a.hidden = true; }, error ? 6000 : 3500);
}

// Pide url cada segundo y llama a pintar(datos). Devuelve una función para refrescar ya.
function sondear(url, pintar, quien) {
  let temporizador = null, enCurso = false;
  async function ciclo() {
    clearTimeout(temporizador);
    if (enCurso) return;
    enCurso = true;
    try {
      const r = await fetch(url, {cache: 'no-store'});
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const datos = await r.json();
      bandaDesconexion(false, quien);
      pintar(datos);
    } catch (err) {
      bandaDesconexion(true, quien);
      if (!(err instanceof TypeError) && !String(err.message).startsWith('HTTP')) console.error(err);
    } finally {
      enCurso = false;
      temporizador = setTimeout(ciclo, 1000);
    }
  }
  ciclo();
  return ciclo;
}

// POST JSON. Muestra el mensaje o el error en un aviso y devuelve la respuesta ({ok, ...}).
async function enviar(url, cuerpo = {}) {
  try {
    const r = await fetch(url, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(cuerpo)});
    const datos = await r.json().catch(() => ({ok: false, error: 'HTTP ' + r.status}));
    if (datos.ok === false) avisar(datos.error || 'No se pudo hacer', true);
    else if (datos.mensaje) avisar(datos.mensaje);
    return datos;
  } catch (err) {
    avisar('No hay conexión con el proceso', true);
    return {ok: false, error: String(err)};
  }
}
