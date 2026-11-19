/* EPMS progressive table pagination: preserves all rows in DOM for existing exports,
   filters, counters, and edit handlers. This is display pagination, NOT server pagination. */
(()=>{'use strict';
const PAGE_SIZE=50,MIN_ROWS=150;const managed=new WeakMap();let scheduled=false;
function eligible(table){if(table.closest('[role="dialog"],dialog,.modal,[hidden]'))return false;
 if(table.matches('[data-epms-no-pagination]'))return false;
 if(table.querySelector('input[type="checkbox"],input[type="radio"],tr[rowspan]'))return false;
 return !!table.tBodies.length && table.tBodies[0].rows.length>=MIN_ROWS;
}
function init(table){if(managed.has(table)||!eligible(table))return;
 const tbody=table.tBodies[0],nav=document.createElement('div');nav.className='epms-page-controls';
 nav.style.cssText='display:flex;align-items:center;justify-content:flex-end;gap:10px;flex-wrap:wrap;margin:12px 0;font:inherit';
 const info=document.createElement('span'),prev=document.createElement('button'),next=document.createElement('button');
 for(const b of [prev,next]){b.type='button';b.style.cssText='border:1px solid #93b4da;background:#edf5ff;color:#15477c;border-radius:8px;padding:7px 12px;cursor:pointer';}
 prev.textContent='‹ Previous';next.textContent='Next ›';nav.append(info,prev,next);table.insertAdjacentElement('afterend',nav);
 const state={page:1,tbody,nav,info,prev,next,signature:'',rendering:false};managed.set(table,state);
 function render(){if(!table.isConnected){nav.remove();return;}const rows=Array.from(tbody.rows);
 // Only rows hidden by application filters are excluded. Our own hidden rows are marked.
 const visible=rows.filter(r=>r.dataset.epmsPageHidden==='1'||(r.style.display!=='none'&&!r.hidden));
 const total=visible.length,pages=Math.max(1,Math.ceil(total/PAGE_SIZE));state.page=Math.min(state.page,pages);
 state.rendering=true;const start=(state.page-1)*PAGE_SIZE;
 for(const r of rows){const shouldShow=visible.includes(r)&&visible.indexOf(r)>=start&&visible.indexOf(r)<start+PAGE_SIZE;
 if(shouldShow){if(r.dataset.epmsPageHidden==='1'){r.style.display=r.dataset.epmsPreviousDisplay||'';delete r.dataset.epmsPageHidden;delete r.dataset.epmsPreviousDisplay;}}
 else if(visible.includes(r)&&r.dataset.epmsPageHidden!=='1'){r.dataset.epmsPreviousDisplay=r.style.display||'';r.dataset.epmsPageHidden='1';r.style.display='none';}}
 info.textContent=`${total?start+1:0}–${Math.min(total,start+PAGE_SIZE)} of ${total}`;prev.disabled=state.page===1;next.disabled=state.page===pages;
 nav.style.display=total>PAGE_SIZE?'flex':'none';state.rendering=false;
 }
 prev.onclick=()=>{state.page--;render();};next.onclick=()=>{state.page++;render();};
 const observer=new MutationObserver(changes=>{if(state.rendering)return;
 // Defer updates; do not respond to own pagination mutations.
 if(changes.some(c=>c.type==='childList'||(c.type==='attributes'&&c.attributeName!=='data-epms-page-hidden'&&!c.target.dataset.epmsPageHidden))){clearTimeout(state.timer);state.timer=setTimeout(()=>{state.page=1;render();},180);}});
 observer.observe(tbody,{childList:true,subtree:true,attributes:true,attributeFilter:['style','hidden']});
 render();}
function scan(){scheduled=false;document.querySelectorAll('table').forEach(init);}
function schedule(){if(!scheduled){scheduled=true;setTimeout(scan,300);}}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',schedule);else schedule();
new MutationObserver(schedule).observe(document.documentElement,{childList:true,subtree:true});
})();