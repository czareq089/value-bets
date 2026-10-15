// Author the embedded template with the bundled Codex artifact-tool runtime.
import fs from 'node:fs/promises';
import { Workbook, SpreadsheetFile } from '@oai/artifact-tool';
const wb = Workbook.create();
const headers = ['Data meczu','Mecz','Zawodnik','Rynek','Kierunek','Linia','Bukmacher','Kurs','Hitrate przed meczem','Próba','Ocena','Wynik gracza','Czy siadło','Minuty','Zapisano','Rozliczono','Grupa rywali','Źródło statystyk','Uwagi','Zysk (1 j.)'];
function buildSheet(book,index,name) {
  const s=book.worksheets.add(name);
  s.showGridLines=false; s.tabColor=index?'#2563EB':'#0F766E';
  s.getRange('A1:Z33').format.font={name:'Aptos',size:11,color:'#172554'};
  s.getRange('A1:T2').merge(); s.getRange('A1').values=[[`VALUE BETS / ${name.toUpperCase()}`]];
  s.getRange('A1:T2').format={fill:'#102A43',font:{size:23,bold:true,color:'#FFFFFF'}};
  s.getRange('A4:T4').merge(); s.getRange('A4').values=[[index?'Wszystkie oferty z ukończonych analiz otwartych w aplikacji Windows.':'Wybory zapisane prawym przyciskiem myszy. Kurs i próbka pozostają z chwili wyboru.']];
  s.getRange('A6:T6').merge(); s.getRange('A6').values=[['Wyniki uzupełnia aplikacja Windows. Brak statystyki pozostaje brakiem danych, a nie zerem.']];
  s.getRange('A8:T8').merge(); s.getRange('A8').values=[['Skoroszyt jest generowany przez aplikację. Dane źródłowe: lokalny bet_journal.json. Zamknij Excel, aby odświeżyć plik.']];
  s.getRange('A10:T10').merge(); s.getRange('A10').values=[['Rozliczenie: statystyka zawodnika w regulaminowym czasie; bez SuperSub. Dotychczasowe hitrate nie jest przeliczane po meczu.']];
  s.getRange('A4:T10').format.font={size:11,color:'#52657A'};
  s.getRange('A12:T12').values=[headers];
  s.getRange('A12:T12').format={fill:'#0F766E',font:{bold:true,color:'#FFFFFF'},rowHeight:38,wrapText:true};
  s.getRange('A13:T13').values=[Array(20).fill(null)];
  s.getRange('A13:T13').format={rowHeight:30,verticalAlignment:'center',font:{color:'#172554'}};
  s.getRange('B13:D13').format.wrapText=true;
  s.getRange('S13').format.wrapText=true;
  s.getRange('A13').setNumberFormat('dd.mm.yyyy hh:mm');
  s.getRange('O13:P13').setNumberFormat('dd.mm.yyyy hh:mm');
  s.getRange('F13').setNumberFormat('0.0'); s.getRange('H13').setNumberFormat('0.00');
  s.getRange('I13').setNumberFormat('0.0%'); s.getRange('J13:N13').setNumberFormat('0');
  s.getRange('T13').setNumberFormat('0.00;[Red]-0.00');
  s.getRange('T13').formulas=[['=IF(M13="TAK",H13*$W$14*$W$15-$W$15,IF(M13="NIE",-$W$15,IF(M13="ZWROT",0,"")))']];
  s.tables.add('A12:T13',true,index?'WszystkieTypy':'MojeWybory').showFilterButton=true;
  s.freezePanes.freezeRows(12); s.freezePanes.freezeColumns(3);
  s.getRange('A:A').format.columnWidth=23; s.getRange('B:D').format.columnWidth=32;
  s.getRange('E:N').format.columnWidth=16; s.getRange('O:P').format.columnWidth=23;
  s.getRange('Q:R').format.columnWidth=19; s.getRange('S:S').format.columnWidth=48;
  s.getRange('T:T').format.columnWidth=16; s.getRange('U:U').format.columnWidth=3;
  s.getRange('V:V').format.columnWidth=28; s.getRange('W:Z').format.columnWidth=16;
  s.getRange('V1:Z2').merge(); s.getRange('V1').values=[['STATYSTYKI']];
  s.getRange('V1:Z2').format={fill:'#102A43',font:{size:18,bold:true,color:'#FFFFFF'}};
  const labels=['Zapisane oferty','Trafione','Nietrafione','Zwroty','Oczekujące','Brak danych','Rozliczone','Skuteczność','Zysk (j.)','ROI'];
  s.getRange('V3:V12').values=labels.map(v=>[v]);
  s.getRange('W3:W12').formulas=[['=COUNTA(C13:C13)'],['=COUNTIF(M13:M13,"TAK")'],['=COUNTIF(M13:M13,"NIE")'],['=COUNTIF(M13:M13,"ZWROT")'],['=COUNTIF(M13:M13,"OCZEKUJE")'],['=COUNTIF(M13:M13,"BRAK DANYCH")'],['=SUM(W4:W6)'],['=IF(W4+W5=0,"—",W4/(W4+W5))'],['=SUM(T13:T13)'],['=IF(W4+W5=0,"—",W11/((W4+W5)*W15))']];
  s.getRange('W10').setNumberFormat('0.0%'); s.getRange('W12').setNumberFormat('0.0%'); s.getRange('W11').setNumberFormat('0.00;[Red]-0.00');
  s.getRange('V14:W15').values=[['Mnożnik wypłaty',0.88],['Stawka symulacji',1]];
  s.getRange('W14').setNumberFormat('0.00');
  for (const [r,text] of [[17,'Zysk i ROI: symulacja, nie saldo konta.'],[18,'Braki danych nie obniżają skuteczności.'],[19,'Zwroty nie zwiększają obrotu ROI.']]) { s.getRange(`V${r}:Z${r}`).merge(); s.getRange(`V${r}`).values=[[text]]; }
  for (const [row,field,items] of [[21,'G',['SUPERBET','BETCLIC','STS']],[28,'K',['VALUE','TRAP','NEUTRAL']]]) {
    s.getRange(`V${row}:Z${row}`).values=[[field==='G'?'Bukmacher':'Ocena','Oferty','Trafione','Nietrafione','Skuteczność']];
    s.getRange(`V${row}:Z${row}`).format={fill:'#DBEAFE',font:{bold:true,color:'#172554'},rowHeight:32,wrapText:true};
    for (const [i,item] of items.entries()) { const r=row+i+1; s.getRange(`V${r}`).values=[[item]]; s.getRange(`W${r}:Z${r}`).formulas=[[`=COUNTIF(${field}13:${field}13,V${r})`,`=COUNTIFS(${field}13:${field}13,V${r},M13:M13,"TAK")`,`=COUNTIFS(${field}13:${field}13,V${r},M13:M13,"NIE")`,`=IF(X${r}+Y${r}=0,"—",X${r}/(X${r}+Y${r}))`]]; s.getRange(`Z${r}`).setNumberFormat('0.0%'); }
  }
  for (const [out,fill] of [['TAK','#DCFCE7'],['NIE','#FEE2E2'],['ZWROT','#E2E8F0'],['BRAK DANYCH','#FEF3C7']]) s.getRange('M13:M13').conditionalFormats.add('containsText',{text:out,format:{fill}});
}
for (const [i,name] of ['Moje wybory','Wszystkie typy'].entries()) buildSheet(wb,i,name);
wb.recalculate();
console.log((await wb.inspect({kind:'region',sheetId:'Moje wybory',range:'V3:W15',maxChars:1800})).ndjson);
console.log((await wb.inspect({kind:'match',searchTerm:'#REF!|#DIV/0!|#VALUE!|#NAME\\?|#NUM!',options:{useRegex:true,maxResults:20},maxChars:1200})).ndjson);
await fs.mkdir('assets',{recursive:true}); await fs.mkdir('tools/journal-preview',{recursive:true});
for (const [i,name] of ['Moje wybory','Wszystkie typy'].entries()) {
  const previewBook=Workbook.create(); buildSheet(previewBook,i,name); previewBook.recalculate();
  const img=await previewBook.render({sheetName:name,range:'V1:Z32',scale:1.5,format:'png'});
  const detail=await previewBook.render({sheetName:name,range:'A1:T15',scale:1,format:'png'});
  await fs.writeFile(`tools/journal-preview/detail-${i+1}.png`,new Uint8Array(await detail.arrayBuffer()));
  await fs.writeFile(`tools/journal-preview/sheet-${i+1}.png`,new Uint8Array(await img.arrayBuffer()));
}
await (await SpreadsheetFile.exportXlsx(wb)).save('assets/bet_journal_template.xlsx');
