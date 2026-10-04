export namespace main {
	
	export class ShotCoordinate {
	    x: number;
	    y: number;
	    shotType: string;
	    inBox: boolean;
	    onTarget: boolean;
	    isSub: boolean;
	
	    static createFrom(source: any = {}) {
	        return new ShotCoordinate(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.x = source["x"];
	        this.y = source["y"];
	        this.shotType = source["shotType"];
	        this.inBox = source["inBox"];
	        this.onTarget = source["onTarget"];
	        this.isSub = source["isSub"];
	    }
	}
	export class AnalyzedBet {
	    bookmaker: string;
	    playerName: string;
	    marketType: string;
	    marketLabel: string;
	    line: number;
	    isUnder: boolean;
	    bookmakerOdds: number;
	    fairOdds: number;
	    hitRate: number;
	    edgePercent: number;
	    evWithTax: number;
	    matchesSample: number;
	    sampleValues: number[];
	    sampleValuesSuperSub: number[];
	    sampleMinutes: number[];
	    samplePositions: string[];
	    sampleOpponents: string[];
	    sampleDates: string[];
	    sampleShots: ShotCoordinate[][];
	    statsSource: string;
	    teamSide: string;
	    teamId: string;
	    teamName: string;
	    verdict: string;
	    lineupStatus: string;
	    isStartingXI: boolean;
	
	    static createFrom(source: any = {}) {
	        return new AnalyzedBet(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.bookmaker = source["bookmaker"];
	        this.playerName = source["playerName"];
	        this.marketType = source["marketType"];
	        this.marketLabel = source["marketLabel"];
	        this.line = source["line"];
	        this.isUnder = source["isUnder"];
	        this.bookmakerOdds = source["bookmakerOdds"];
	        this.fairOdds = source["fairOdds"];
	        this.hitRate = source["hitRate"];
	        this.edgePercent = source["edgePercent"];
	        this.evWithTax = source["evWithTax"];
	        this.matchesSample = source["matchesSample"];
	        this.sampleValues = source["sampleValues"];
	        this.sampleValuesSuperSub = source["sampleValuesSuperSub"];
	        this.sampleMinutes = source["sampleMinutes"];
	        this.samplePositions = source["samplePositions"];
	        this.sampleOpponents = source["sampleOpponents"];
	        this.sampleDates = source["sampleDates"];
	        this.sampleShots = this.convertValues(source["sampleShots"], ShotCoordinate);
	        this.statsSource = source["statsSource"];
	        this.teamSide = source["teamSide"];
	        this.teamId = source["teamId"];
	        this.teamName = source["teamName"];
	        this.verdict = source["verdict"];
	        this.lineupStatus = source["lineupStatus"];
	        this.isStartingXI = source["isStartingXI"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class LeagueOption {
	    id: number;
	    name: string;
	
	    static createFrom(source: any = {}) {
	        return new LeagueOption(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	    }
	}
	export class MatchHistoryEntry {
	    timestamp: string;
	    title: string;
	    sbUrl: string;
	    betclicUrl: string;
	    stsUrl: string;
	    homeStatsId: string;
	    awayStatsId: string;
	    sampleLimit?: number;
	
	    static createFrom(source: any = {}) {
	        return new MatchHistoryEntry(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.timestamp = source["timestamp"];
	        this.title = source["title"];
	        this.sbUrl = source["sbUrl"];
	        this.betclicUrl = source["betclicUrl"];
	        this.stsUrl = source["stsUrl"];
	        this.homeStatsId = source["homeStatsId"];
	        this.awayStatsId = source["awayStatsId"];
	        this.sampleLimit = source["sampleLimit"];
	    }
	}
	export class MyBetItem {
	    id: string;
	    bookmaker: string;
	    playerName: string;
	    marketType: string;
	    marketLabel: string;
	    line: number;
	    isUnder: boolean;
	    bookmakerOdds: number;
	    fairOdds: number;
	    hitRate: number;
	    edgePercent: number;
	    evWithTax: number;
	    matchesSample: number;
	    sampleValues: number[];
	    sampleValuesSuperSub: number[];
	    sampleMinutes: number[];
	    samplePositions: string[];
	    sampleOpponents: string[];
	    sampleDates: string[];
	    sampleShots: ShotCoordinate[][];
	    statsSource: string;
	    teamSide: string;
	    teamId: string;
	    teamName: string;
	    verdict: string;
	    lineupStatus: string;
	    isStartingXI: boolean;
	
	    static createFrom(source: any = {}) {
	        return new MyBetItem(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.bookmaker = source["bookmaker"];
	        this.playerName = source["playerName"];
	        this.marketType = source["marketType"];
	        this.marketLabel = source["marketLabel"];
	        this.line = source["line"];
	        this.isUnder = source["isUnder"];
	        this.bookmakerOdds = source["bookmakerOdds"];
	        this.fairOdds = source["fairOdds"];
	        this.hitRate = source["hitRate"];
	        this.edgePercent = source["edgePercent"];
	        this.evWithTax = source["evWithTax"];
	        this.matchesSample = source["matchesSample"];
	        this.sampleValues = source["sampleValues"];
	        this.sampleValuesSuperSub = source["sampleValuesSuperSub"];
	        this.sampleMinutes = source["sampleMinutes"];
	        this.samplePositions = source["samplePositions"];
	        this.sampleOpponents = source["sampleOpponents"];
	        this.sampleDates = source["sampleDates"];
	        this.sampleShots = this.convertValues(source["sampleShots"], ShotCoordinate);
	        this.statsSource = source["statsSource"];
	        this.teamSide = source["teamSide"];
	        this.teamId = source["teamId"];
	        this.teamName = source["teamName"];
	        this.verdict = source["verdict"];
	        this.lineupStatus = source["lineupStatus"];
	        this.isStartingXI = source["isStartingXI"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class PrefetchProgress {
	    running: boolean;
	    day: string;
	    completed: number;
	    total: number;
	    cached: number;
	    skipped: number;
	    failed: number;
	    currentMatch: string;
	    playersDone: number;
	    playersTotal: number;
	    message: string;
	
	    static createFrom(source: any = {}) {
	        return new PrefetchProgress(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.running = source["running"];
	        this.day = source["day"];
	        this.completed = source["completed"];
	        this.total = source["total"];
	        this.cached = source["cached"];
	        this.skipped = source["skipped"];
	        this.failed = source["failed"];
	        this.currentMatch = source["currentMatch"];
	        this.playersDone = source["playersDone"];
	        this.playersTotal = source["playersTotal"];
	        this.message = source["message"];
	    }
	}
	
	export class SuperbetCalendarEvent {
	    id: string;
	    home: string;
	    away: string;
	    competition: string;
	    country: string;
	    startTime: string;
	    url: string;
	    live: boolean;
	    betBuilder: boolean;
	
	    static createFrom(source: any = {}) {
	        return new SuperbetCalendarEvent(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.home = source["home"];
	        this.away = source["away"];
	        this.competition = source["competition"];
	        this.country = source["country"];
	        this.startTime = source["startTime"];
	        this.url = source["url"];
	        this.live = source["live"];
	        this.betBuilder = source["betBuilder"];
	    }
	}
	export class SuperbetCalendar {
	    date: string;
	    events: SuperbetCalendarEvent[];
	    warnings: string[];
	    fetchedAt?: string;
	
	    static createFrom(source: any = {}) {
	        return new SuperbetCalendar(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.date = source["date"];
	        this.events = this.convertValues(source["events"], SuperbetCalendarEvent);
	        this.warnings = source["warnings"];
	        this.fetchedAt = source["fetchedAt"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	export class TeamDetectionResult {
	    homeSlug: string;
	    awaySlug: string;
	    homeId: string;
	    awayId: string;
	
	    static createFrom(source: any = {}) {
	        return new TeamDetectionResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.homeSlug = source["homeSlug"];
	        this.awaySlug = source["awaySlug"];
	        this.homeId = source["homeId"];
	        this.awayId = source["awayId"];
	    }
	}

}

