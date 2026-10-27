#!/usr/bin/env python3
"""Offline report generator; independent of all benchmark runners."""
from pathlib import Path
import csv,json,html,sys,subprocess,os,re
os.environ.setdefault("SOURCE_DATE_EPOCH", "1789170841")
ZT_ONLY = "--ztreamer-only" in sys.argv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
REPO=Path(__file__).resolve().parents[1]
SITE=REPO/'site'
ASSETS='benchmarks/2026-09-11/'
O=SITE/ASSETS
def read(p):return list(csv.DictReader(p.open()))
ROWS=read(O/'summary.csv')
SAMPLES=read(O/'samples.csv')
CURVE=read(O/'height-time.csv')
INDEX_CURVE=read(O/'ztreamer-height-time.csv')
INDEX_RUNS=json.loads((O/'indexing-metadata.json').read_text())['runs']
HARDWARE=json.loads((O/'hardware.json').read_text())
METADATA=json.loads((O/'metadata.json').read_text())
V=['86bf4b0-limit16','v0.0.1','zaino-d27ec930']
L=dict(zip(V,['ztreamer v0.1.0','ztreamer v0.0.1','zaino d27ec930']))
C=dict(zip(V,['#008dc1','#e87500','#c77da9']))
if ZT_ONLY: V=V[:2]
plt.rcParams.update({'font.family':'DejaVu Sans Mono','font.size':10,'axes.titlesize':12,'axes.titleweight':'normal','axes.titlepad':12,'text.color':'#dbcda4','axes.labelcolor':'#dbcda4','xtick.color':'#dbcda4','ytick.color':'#dbcda4','axes.edgecolor':'#dbcda4','axes.spines.top':True,'axes.spines.right':True,'figure.facecolor':'#262626','axes.facecolor':'#262626','savefig.facecolor':'#262626','grid.color':'#dbcda4','grid.alpha':.10,'legend.facecolor':'#262626','legend.edgecolor':'#dbcda4','svg.fonttype':'none'})

def row(v,suite,case=None,clients=None):return next(x for x in ROWS if x['label']==v and x['suite']==suite and (case is None or x['case']==case) and (clients is None or int(x['clients'])==clients))
def val(r,k,scale=1):return float(r[k])/scale if r['status']=='ok' and r[k] else np.nan
def style(ax,title,ylabel='',log=False):
 ax.set_title(title,loc='left');ax.set_ylabel(ylabel);ax.grid(axis='y');ax.set_axisbelow(True)
 if log:ax.set_yscale('log')
def figgrid(title,subtitle,nrows=1,ncols=2,size=(15,6.5),legend=True):
 fig,axes=plt.subplots(nrows,ncols,figsize=size,squeeze=False)
 fig.subplots_adjust(left=.085,right=.97,bottom=.12,top=.79 if nrows==1 else .85,hspace=.5,wspace=.28)
 fig.suptitle(title,x=.06,y=.97,ha='left',fontsize=22,fontweight='bold')
 if legend:
  fig.legend(handles=[plt.Line2D([0],[0],color=C[v],marker='o',lw=2,label=L[v]) for v in V],loc='upper left',bbox_to_anchor=(.055,.97-.47/size[1]),frameon=False,ncol=3)
 return fig,axes.ravel()
def lines(ax,suite,metric,scale=1,by='size'):
 xs=[100,1000,10000,100000] if by=='size' else [1,8,32,128]
 for v in V:
  ys=[val(row(v,suite,f'GetBlockRange/{x}' if by=='size' else None,x if by!='size' else None),metric,scale) for x in xs]
  ax.plot(range(4),ys,color=C[v],marker='o',lw=2.3,label=L[v])
 ax.set_xticks(range(4),['100','1k','10k','100k'] if by=='size' else xs)
 ax.set_xlabel('Blocks per request' if by=='size' else 'Concurrent clients')
def save(fig,name):
 fig.savefig(O/f'{name}{"-ztreamer" if ZT_ONLY else ""}.svg',bbox_inches='tight')
 plt.close(fig)

fig,a=figgrid('ztreamer v0.1.0 vs v0.0.1' if ZT_ONLY else 'Serving performance: zaino and ztreamer','One selected repeat each · identical benchmark interval and client · sequential server runs',2,2,(15,10.5))
lines(a[0],'ranges','complete_p50_us',1000);style(a[0],'Range median latency ↓','Milliseconds · log scale',True)
lines(a[1],'concurrency','blocks_per_second',1e6,'clients');style(a[1],'Concurrent throughput ↑','Million blocks / second');a[1].set_ylim(bottom=0)
lines(a[2],'concurrency','complete_p99_us',1e6,'clients');style(a[2],'Concurrent p99 latency ↓','Complete response · seconds');a[2].set_ylim(bottom=0)
for v in V:
 y=val(row(v,'concurrency',clients=128),'complete_p99_us',1e6);a[2].annotate(f'{y:.2f} s',(3,y),xytext=(-6,8),textcoords='offset points',ha='right',color=C[v])
times=[val(row(v,'sync'),'wall_seconds') for v in V]
a[3].barh(range(len(V)),times,color=[C[v] for v in V]);a[3].set_yticks(range(len(V)),[L[v] for v in V]);a[3].invert_yaxis()
for i,t in enumerate(times):a[3].text(t+3,i,f'{t:.2f} s',va='center',color=C[V[i]])
a[3].set_xlim(0,max(times)*1.27);style(a[3],'Wallet download time ↓');a[3].set_xlabel('Seconds · excludes wallet scanning / trial decryption')
save(fig,'overview')

cases=[x['case'] for x in ROWS if x['label']==V[0] and x['suite']=='latency']
fig,a=figgrid('RPC latency: every measured method','100 successful requests per supported case · completion includes stream draining and client decoding',size=(16,12))
fig.subplots_adjust(left=.29,right=.97,bottom=.09,top=.87,wspace=.18)
for ax,metric,title in zip(a,['complete_p50_us','complete_p99_us'],['p50 · median ↓','p99 · tail ↓']):
 for i,c in enumerate(cases):
  ys=[val(row(v,'latency',c),metric,1000) for v in V];valid=[y for y in ys if np.isfinite(y)]
  if valid:ax.plot([min(valid),max(valid)],[i,i],color='#6a6454',lw=1.3)
  for v,y in zip(V,ys):
   if np.isfinite(y):ax.scatter(y,i,color=C[v],s=40)
 ax.set_xscale('log');ax.set_yticks(range(len(cases)),cases if ax is a[0] else ['']*len(cases));ax.invert_yaxis();ax.grid(axis='x');ax.set_title(title,loc='left');ax.set_xlabel('Milliseconds · log scale')
save(fig,'rpc-latencies')

fig,a=figgrid('Range scaling','Samples per size: 100 → 1,000; 1k → 1,000; 10k → 1,000; 100k → 100',ncols=3,size=(16,6.5))
for ax,k,scale,title,unit,log in zip(a,['first_p50_us','complete_p99_us','blocks_per_second'],[1000,1e6,1000],['First-block median ↓','Whole-range p99 ↓','Aggregate throughput ↑'],['Milliseconds · log','Seconds · log','Thousand blocks / second'],[True,True,False]):
 lines(ax,'ranges',k,scale);style(ax,title,unit,log)
 if not log:ax.set_ylim(bottom=0)
save(fig,'range-details')

fig,a=figgrid('Concurrent serving: latency and throughput','10,000 blocks/request · 1 / 8 / 32 / 128 connected clients · one outstanding request per client',ncols=3,size=(16,6.5))
for ax,k,scale,title,unit in zip(a,['first_p99_us','complete_p50_us','protobuf_bytes_per_second'],[1e6,1e6,1e9],['First-block p99 ↓','Whole-response median ↓','Protobuf throughput ↑'],['Seconds','Seconds','GB / second · decimal']):
 lines(ax,'concurrency',k,scale,'clients');style(ax,title,unit);ax.set_ylim(bottom=0)
save(fig,'concurrency')

fig,a=figgrid('Wallet download by block height','306 sequential chunks · 3,059,644 compact blocks · final chunk: 9,644 blocks')
for v in V:
 xs=sorted([x for x in SAMPLES if x['label']==v and x['suite']=='sync' and x['phase']=='measure'],key=lambda x:int(x['sample']))
 assert len(xs)==306 and all(x['status']=='ok' for x in xs)
 h=np.array([int(x['end_height']) for x in xs])/1e6;t=np.array([float(x['complete_us']) for x in xs])/1e6
 a[0].plot(np.r_[.4192,h],np.r_[0,t.cumsum()],color=C[v],lw=2.3)
 a[1].plot(h,t,color=C[v],lw=1.1)
style(a[0],'Cumulative request time ↓','Seconds');style(a[1],'Per-chunk time ↓','Seconds · log',True)
for ax in a:ax.set_xlabel('Mainnet height · millions');ax.set_xlim(.4192,3.478843)
save(fig,'wallet-download')

# Historical indexing is collected separately from the serving measurements.
index_colors={'v0.1.0':C['86bf4b0-limit16'],'v0.0.1':C['v0.0.1']}
index_runs={r['label']:r for r in INDEX_RUNS}
index_curves={v:sorted([r for r in INDEX_CURVE if r['label']==v],key=lambda r:float(r['elapsed_s'])) for v in index_colors}
fig,a=figgrid('Historical indexing: height against time','',size=(15,7),legend=False)
for v,xs in index_curves.items():
 points=[r for r in xs if r['db_tip_height']]+[dict(elapsed_s=index_runs[v]['ready_s'],db_tip_height=index_runs[v]['final_height'])]
 for ax,scale in [(a[0],1 if ZT_ONLY else 3600),(a[1],1)]:
  ax.step([float(r['elapsed_s'])/scale for r in points],[int(r['db_tip_height'])/1e6 for r in points],where='post',color=index_colors[v],lw=1.8,label=f"ztreamer {v} · {index_runs[v]['ready_s']:.1f} s")
if not ZT_ONLY:
 points=[r for r in CURVE if r['db_tip_height']]
 points+=[dict(elapsed_s=METADATA['zaino_sync']['elapsed_s'],db_tip_height=points[-1]['db_tip_height'])]
 a[0].step([float(r['elapsed_s'])/3600 for r in points],[int(r['db_tip_height'])/1e6 for r in points],where='post',color=C['zaino-d27ec930'],lw=1.8,label=f"zaino · {METADATA['zaino_sync']['elapsed_s']/3600:.2f} h")
for ax,title,unit in [(a[0],'Full clean indexes','Seconds' if ZT_ONLY else 'Hours'),(a[1],'ztreamer detail','Seconds')]:
 style(ax,title,'Committed height · millions');ax.set_xlabel(f'{unit} since process start');ax.set_ylim(bottom=0);ax.set_xlim(left=0);ax.legend(loc='lower right',fontsize=8)
if ZT_ONLY:
 a[1].clear()
 for i,(v,r) in enumerate(index_runs.items()):
  a[1].barh(i,r['ready_s'],color=index_colors[v],alpha=.35,height=.42)
  a[1].barh(i,r['historical_duration_s'],left=r['historical_start_s'],color=index_colors[v],height=.42)
  a[1].text(r['ready_s']+2,i,f"{r['ready_s']:.1f} s",va='center',color=index_colors[v])
 a[1].set_yticks(range(len(index_runs)),list(index_runs));a[1].set_xlim(0,max(r['ready_s'] for r in INDEX_RUNS)*1.25)
 a[1].set_ylim(-.6,1.8)
 style(a[1],'Startup through serving');a[1].set_xlabel('Seconds since process start')
 a[1].legend(handles=[plt.Rectangle((0,0),1,1,color='#dbcda4',alpha=.35,label='Startup / readiness'),plt.Rectangle((0,0),1,1,color='#dbcda4',label='Historical indexing')],loc='upper right',fontsize=8)
save(fig,'historical-indexing')

fig,a=figgrid('Resources during historical indexing','',nrows=1 if ZT_ONLY else 2,ncols=3,size=(16,6.5 if ZT_ONLY else 11))
def resource_row(axes,series,scale,unit):
 for label,xs,color in series:
  xs=[r for r in xs if r['rss_bytes'] and r['cpu_seconds']]
  t=np.array([float(r['elapsed_s']) for r in xs]);cpu=np.array([float(r['cpu_seconds']) for r in xs])
  axes[0].plot(t/scale,[float(r['rss_bytes'])/2**30 for r in xs],color=color,label=label)
  # Ten-second CPU windows, including the final partial window.
  idx=[0]
  for i in range(1,len(t)):
   if t[i]-t[idx[-1]]>=10:idx.append(i)
  if idx[-1]!=len(t)-1:idx.append(len(t)-1)
  axes[1].plot(t[idx][1:]/scale,np.diff(cpu[idx])/np.diff(t[idx]),color=color,label=label)
  for key,kind,ls in [('read_bytes','read','-'),('write_bytes','written','--')]:
   axes[2].plot(t/scale,[float(r[key] or 0)/2**30 for r in xs],color=color,ls=ls,label=f'{label} {kind}')
 for ax,title,y in zip(axes,['Resident memory','Average CPU cores used','Cumulative process disk I/O'],['GiB','CPU seconds / elapsed second','GiB']):
  style(ax,title,y);ax.set_ylim(bottom=0);ax.set_xlabel(f'{unit} since process start')
 axes[0].legend(fontsize=8);axes[2].legend(fontsize=8)
resource_row(a[:3],[(f'ztreamer {v}',xs,index_colors[v]) for v,xs in index_curves.items()],1,'Seconds')
if not ZT_ONLY:resource_row(a[3:],[('zaino',CURVE,C['zaino-d27ec930'])],3600,'Hours')
save(fig,'sync-resources')

if ZT_ONLY:
 print('Built ztreamer-only plot variants');sys.exit(0)
subprocess.run([sys.executable,str(Path(__file__).resolve()),'--ztreamer-only'],check=True)

# Standalone page: embedded figures, downloadable raw data, searchable table.
def img(name):
 return f'<img loading="lazy" alt="{name.replace("-", " ")}" src="{ASSETS}{name}.svg?v=20260913">'

def fmt(x):return '—' if not np.isfinite(x) else f'{x:,.3f}'
columns=[('Version',None,1),('Suite',None,1),('Case',None,1),('Clients',None,1),('Successful n',None,1),('Status',None,1),('p50 ms','complete_p50_us',1000),('p95 ms','complete_p95_us',1000),('p99 ms','complete_p99_us',1000),('Max ms','complete_max_us',1000),('First p50 ms','first_p50_us',1000),('First p99 ms','first_p99_us',1000),('Blocks/s','blocks_per_second',1),('MB/s','protobuf_bytes_per_second',1e6),('Wall s','wall_seconds',1)]
table=[]
for r in ROWS:
 status='unsupported' if r['status']=='warmup_failed' and r['case']=='GetMempoolTx' else r['status']
 cells=[L[r['label']],r['suite'],r['case'],r['clients'],r['successes'],status]+[fmt(val(r,k,scale)) for _,k,scale in columns[6:]]
 table.append(('<tr data-zaino hidden>' if r['label']=='zaino-d27ec930' else '<tr>')+''.join('<td>'+html.escape(str(x))+'</td>' for x in cells)+'</tr>')
sections=[('overview','Serving comparison'),('rpc-latencies','RPC latencies'),('range-details','Range sizes'),('concurrency','Concurrent serving'),('wallet-download','Wallet download'),('historical-indexing','Historical indexing'),('sync-resources','Indexing resources')]
zh=val(row(V[2],'ranges','GetBlockRange/10000'),'complete_p50_us',1000);hh=val(row(V[0],'ranges','GetBlockRange/10000'),'complete_p50_us',1000)
zp=val(row(V[2],'concurrency',clients=128),'complete_p99_us',1e6);hp=val(row(V[0],'concurrency',clients=128),'complete_p99_us',1e6)
zw=times[2];hw=times[0]
head=(SITE/'index.html').read_text().split('<head>',1)[1].split('</head>',1)[0]
head=re.sub(r'<title>.*?</title>', '<title>ztreamer benchmarks</title>', head)
head=re.sub(r'<meta (?:name="description"|property="og:description") content="[^"]*">', '', head)
head=head.replace('content="ztreamer"','content="ztreamer benchmarks"')
head+='<meta name="description" content="RPC, range, concurrency and wallet-download benchmarks for ztreamer v0.1.0 and v0.0.1, with optional zaino comparison.">\n<link rel="stylesheet" href="benchmarks.css?v=2">'
page='<!doctype html><html lang="en"><head>'+head+'</head><body class="benchmarks-page">'
page+='<header class="wrap"><a href="./">ztreamer</a><nav><a href="docs.html">docs</a><a href="benchmarks.html" aria-current="page">benchmarks</a><a href="https://github.com/cypherpunktech/ztreamer">source</a></nav></header>'
page+='<main class="wrap benchmark-report">'
page+='<div class="eyebrow">Measured September 11–13, 2026</div><h1>ztreamer v0.1.0 vs v0.0.1</h1><p class="sub">Serving and clean historical indexing benchmarks. Compare <strong>ztreamer v0.1.0</strong> and <strong>v0.0.1</strong>, or include <strong>zaino d27ec930</strong> with the toggle below.</p>'
page+='<label class="compare-toggle"><input id="show-zaino" type="checkbox" role="switch"> Show zaino in comparisons</label>'
page+='<nav class="report-nav" aria-label="Benchmark sections">'+''.join(f'<a href="#{n}">{t}</a>' for n,t in sections)+'<a href="#hardware">Hardware</a><a href="#numbers">All values</a><a href="#data">Raw data</a></nav>'
page+='<div class="cards" data-zaino-only hidden>'+''.join(f'<div class="card">{title}<b>{body}</b><span>{detail}</span></div>' for title,body,detail in [('10,000-block median',f'{zh/hh:.1f}× latency',f'Zaino {zh:.2f} ms · ztreamer v0.1.0 {hh:.2f} ms'),('128-client p99',f'{zp/hp:.2f}× latency',f'Zaino {zp:.2f} s · ztreamer v0.1.0 {hp:.2f} s'),('Wallet download',f'{zw/hw:.2f}× duration',f'Zaino {zw:.2f} s · ztreamer v0.1.0 {hw:.2f} s')])+'</div>'
release_range=val(row(V[1],'ranges','GetBlockRange/10000'),'complete_p50_us',1000)
release_p99=val(row(V[1],'concurrency',clients=128),'complete_p99_us',1e6)
release_wallet=val(row(V[1],'sync'),'wall_seconds')
page+='<div class="cards" data-ztreamer-only>'+''.join(f'<div class="card">{title}<b>{100*(1-new/old):.1f}% lower</b><span>v0.1.0 {new:.2f} {unit} · v0.0.1 {old:.2f} {unit}</span></div>' for title,new,old,unit in [('10,000-block median',hh,release_range,'ms'),('128-client p99',hp,release_p99,'s'),('Wallet download',hw,release_wallet,'s')])+'</div>'
page+='<p class="note" data-ztreamer-only>Same Zakura snapshot, fixed interval and native client. One selected repeat per version; warm connections, no cache reset. Serving v0.1.0 was measured at commit 86bf4b0 with the 16-stream fix. Historical indexing uses the actual v0.1.0 release tag.</p>'
page+='<p class="note" data-zaino-only hidden><strong>Read the comparison correctly.</strong> All serving tests use mainnet heights 419,200–3,478,843, the same fixtures and seed, and sequential runs. Zaino reads a Zebra validator at tip 3,479,843; ztreamer used Zakura at tip 3,478,843. The benchmark upper-block hash matches, but tip-dependent RPCs see different tips. Zaino’s persistent index is complete through 3,478,842; the final benchmark block is in its nonfinalized window. Results include the different backing-node implementations.</p>'
cpu=HARDWARE['cpu'];disk=HARDWARE['benchmark_storage'];memory=HARDWARE['memory']
hardware_rows=[
 ('CPU',f"{cpu['model']} · {cpu['physical_cores']} cores / {cpu['logical_cpus']} threads"),
 ('CPU cache',f"L3: {cpu['l3_cache']}"),
 ('Memory',f"{memory['os_total_bytes']/2**30:.1f} GiB OS-reported RAM · {memory['configured_swap_bytes']/2**30:.1f} GiB configured swap"),
 ('Database / results disk',f"{disk['model']} · {disk['transport'].upper()} SSD · {disk['capacity_bytes']/10**12:.2f} TB ({disk['capacity_bytes']/2**30:.1f} GiB)"),
 ('Filesystem',f"{disk['filesystem']} · {disk['partition']} mounted at {disk['mount_point']}"),
 ('Operating system',f"{HARDWARE['os']['distribution']} · Linux {HARDWARE['os']['kernel']} · {cpu['architecture']}"),
 ('Execution',HARDWARE['execution']),
]
page+='<section id="hardware"><h2>Hardware and environment</h2><div class="table-wrap"><table aria-label="Benchmark hardware"><tbody>'+''.join('<tr><th scope="row">'+html.escape(k)+'</th><td style="text-align:left;white-space:normal">'+html.escape(v)+'</td></tr>' for k,v in hardware_rows)+'</tbody></table></div><p class="downloads">Collected '+html.escape(HARDWARE['collected_at_utc'][:10])+' after the runs on the same host. RAM is the OS-reported total. Clock speeds and runtime settings were not reconstructed. <a href="benchmarks/2026-09-11/hardware.json">Hardware JSON</a></p></section>'
for name,title in sections:
 page+=f'<section id="{name}"><h2>{title}</h2>'
 if name=='historical-indexing':
  page+='<div class="table-wrap"><table aria-label="Historical indexing timings"><thead><tr><th>Version</th><th>Final height</th><th>Indexing seconds</th><th>Launch to serving seconds</th><th>Peak RSS GiB</th></tr></thead><tbody>'
  for r in read(O/'indexing-summary.csv'):
   cells=[r['label'],f"{int(r['final_height']):,}",f"{float(r['historical_duration_s']):,.2f}" if r['historical_duration_s'] else '—',f"{float(r['ready_s']):,.2f}",f"{float(r['peak_rss_bytes'])/2**30:.2f}"]
   page+=('<tr data-zaino-only hidden>' if r['label'].startswith('zaino') else '<tr>')+''.join('<td>'+html.escape(c)+'</td>' for c in cells)+'</tr>'
  page+='</tbody></table></div>'
 page+='<div data-ztreamer-only>'+img(name+'-ztreamer')+'</div><div data-zaino-only hidden>'+img(name)+'</div>'
 page+='</section>'
page+='<details><summary>Methodology and limits</summary><p>One selected repeat per version; no confidence intervals or significance claims. Warmups are excluded. Range counts are 1,000 / 1,000 / 1,000 / 100. RPCs use 100 samples. Concurrency uses ten seconds issuing requests followed by draining in-flight responses, so request populations differ. Wallet download uses 306 chunks and excludes scanning and trial decryption. No cache reset.</p><p>The “busy” address fixture returned no history, and “quiet” returned two transactions per request. Both have zero balance and no UTXOs. These are not heavy address scans. GetMempoolTx returned an empty stream on zaino and v0.1.0; release does not support it. Zaino completed all 27 scenarios with no errors.</p><p>Historical indexing starts with empty persistent indexes. The tagged ztreamer v0.0.1 and v0.1.0 binaries ran sequentially on September 13 against the same static Zakura snapshot, with eight fetch workers and no page-cache reset. The time axes start at process launch; ztreamer includes embedded Zakura startup, and its resources include that node. Zaino’s separate Zebra validator startup and resources are excluded; zaino startup and accumulator rebuild are included in its readiness time. Compilation is excluded for all versions. Final indexed heights differ slightly; exact boundaries and phase times are in the indexing metadata and summary. Height curves show committed progress, sampled once per second. CPU use is averaged over ten-second windows. The ztreamer and zaino resource rows use seconds and hours respectively.</p></details>'
page+='<section id="numbers"><h2>All measured values</h2><p>Latency in milliseconds. Throughput excludes transport framing. Missing values remain absent.</p><input id="filter" placeholder="Filter method, suite, or version" aria-label="Filter results"><span id="count" aria-live="polite"></span><div class="table-wrap"><table><thead><tr>'+''.join('<th>'+x[0]+'</th>' for x in columns)+'</tr></thead><tbody>'+''.join(table)+'</tbody></table></div></section>'
page+='<section id="data"><h2>Data for your own plots</h2><p>'+ ' · '.join(f'<a href="{ASSETS}{f}">{label}</a>' for f,label in [('summary.csv','All scenario summaries'),('samples.csv','All serving samples'),('ztreamer-height-time.csv','ztreamer height/time samples'),('height-time.csv','Zaino height/time samples'),('indexing-summary.csv','Indexing summary'),('indexing-metadata.json','Indexing run metadata'),('metadata.json','Run metadata'),('hardware.json','Hardware'),('README.md','Dataset notes')])+'</p></section></main><footer class="wrap"><a href="https://github.com/zakura-core/zakura">built on zakura</a><a href="benchmarks/2026-09-11/README.md">benchmark dataset</a></footer>'
page+='<script src="benchmarks.js" defer></script></body></html>'
(SITE/'benchmarks.html').write_text(page)
print('Built',SITE/'benchmarks.html','with',len(ROWS),'scenarios and',len(CURVE),'indexing samples')
