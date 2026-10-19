export namespace main {
	
	export class AnalysisEligibility {
	    status: string;
	    reason?: string;
	    links: Record<string, string>;
	    // Go type: time
	    expiresAt?: any;
	
	    static createFrom(source: any = {}) {
	        return new AnalysisEligibility(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.status = source["status"];
	        this.reason = source["reason"];
	        this.links = source["links"];
	        this.expiresAt = this.convertValues(source["expiresAt"], null);
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
	export class FairModelDetails {
	    version: string;
	    distribution: string;
	    probability: number;
	    baseMean: number;
	    expectedCount: number;
	    matchupFactor: number;
	    effectiveSample: number;
	    contextMatches: number;
	    role: string;
	    venue: string;
	    teamId: string;
	    opponentId: string;
	    features: string[];
	    notes: string[];
	
	    static createFrom(source: any = {}) {
	        return new FairModelDetails(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.version = source["version"];
	        this.distribution = source["distribution"];
	        this.probability = source["probability"];
	        this.baseMean = source["baseMean"];
	        this.expectedCount = source["expectedCount"];
	        this.matchupFactor = source["matchupFactor"];
	        this.effectiveSample = source["effectiveSample"];
	        this.contextMatches = source["contextMatches"];
	        this.role = source["role"];
	        this.venue = source["venue"];
	        this.teamId = source["teamId"];
	        this.opponentId = source["opponentId"];
	        this.features = source["features"];
	        this.notes = source["notes"];
	    }
	}
	export class AnalyzedBet {
	    fixtureId?: number;
	    // Go type: time
	    matchStart?: any;
	    matchName?: string;
	    sampleEventIds?: number[];
	    historyNotes?: string[];
	    analysisId?: string;
	    playerId: number;
	    contextPending?: boolean;
	    sampleRatings: number[];
	    fairModel?: FairModelDetails;
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
	    sampleSuperSubAvailable?: boolean[];
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
	        this.fixtureId = source["fixtureId"];
	        this.matchStart = this.convertValues(source["matchStart"], null);
	        this.matchName = source["matchName"];
	        this.sampleEventIds = source["sampleEventIds"];
	        this.historyNotes = source["historyNotes"];
	        this.analysisId = source["analysisId"];
	        this.playerId = source["playerId"];
	        this.contextPending = source["contextPending"];
	        this.sampleRatings = source["sampleRatings"];
	        this.fairModel = this.convertValues(source["fairModel"], FairModelDetails);
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
	        this.sampleSuperSubAvailable = source["sampleSuperSubAvailable"];
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
	export class AnalysisRequest {
	    superbetUrl: string;
	    betclicUrl: string;
	    stsUrl: string;
	    homeId: string;
	    awayId: string;
	    matchLimit: number;
	    tournamentIds: string;
	
	    static createFrom(source: any = {}) {
	        return new AnalysisRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.superbetUrl = source["superbetUrl"];
	        this.betclicUrl = source["betclicUrl"];
	        this.stsUrl = source["stsUrl"];
	        this.homeId = source["homeId"];
	        this.awayId = source["awayId"];
	        this.matchLimit = source["matchLimit"];
	        this.tournamentIds = source["tournamentIds"];
	    }
	}
	export class AnalysisJob {
	    fixtureId?: string;
	    // Go type: time
	    startTime?: any;
	    resultRevision: number;
	    id: string;
	    match: string;
	    status: string;
	    stage: string;
	    message: string;
	    playersDone: number;
	    playersTotal: number;
	    // Go type: time
	    createdAt: any;
	    // Go type: time
	    startedAt?: any;
	    // Go type: time
	    finishedAt?: any;
	    request: AnalysisRequest;
	    warnings: string[];
	    result?: AnalyzedBet[];
	    error?: string;
	
	    static createFrom(source: any = {}) {
	        return new AnalysisJob(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.fixtureId = source["fixtureId"];
	        this.startTime = this.convertValues(source["startTime"], null);
	        this.resultRevision = source["resultRevision"];
	        this.id = source["id"];
	        this.match = source["match"];
	        this.status = source["status"];
	        this.stage = source["stage"];
	        this.message = source["message"];
	        this.playersDone = source["playersDone"];
	        this.playersTotal = source["playersTotal"];
	        this.createdAt = this.convertValues(source["createdAt"], null);
	        this.startedAt = this.convertValues(source["startedAt"], null);
	        this.finishedAt = this.convertValues(source["finishedAt"], null);
	        this.request = this.convertValues(source["request"], AnalysisRequest);
	        this.warnings = source["warnings"];
	        this.result = this.convertValues(source["result"], AnalyzedBet);
	        this.error = source["error"];
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
	export class MatchMarketAvailability {
	    bookmaker: string;
	    eventId: string;
	    status: string;
	    markets: string[];
	    // Go type: time
	    checkedAt: any;
	    // Go type: time
	    expiresAt: any;
	    error?: string;
	    home?: string;
	    away?: string;
	    // Go type: time
	    startTime?: any;
	
	    static createFrom(source: any = {}) {
	        return new MatchMarketAvailability(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.bookmaker = source["bookmaker"];
	        this.eventId = source["eventId"];
	        this.status = source["status"];
	        this.markets = source["markets"];
	        this.checkedAt = this.convertValues(source["checkedAt"], null);
	        this.expiresAt = this.convertValues(source["expiresAt"], null);
	        this.error = source["error"];
	        this.home = source["home"];
	        this.away = source["away"];
	        this.startTime = this.convertValues(source["startTime"], null);
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
	export class MatchPreparation {
	    qualified: boolean;
	    reason?: string;
	    links: Record<string, string>;
	    sources: Record<string, MatchMarketAvailability>;
	
	    static createFrom(source: any = {}) {
	        return new MatchPreparation(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.qualified = source["qualified"];
	        this.reason = source["reason"];
	        this.links = source["links"];
	        this.sources = this.convertValues(source["sources"], MatchMarketAvailability, true);
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
	export class MyBetItem {
	    fixtureId?: number;
	    // Go type: time
	    matchStart?: any;
	    matchName?: string;
	    addedAt?: string;
	    includeSuperSub: boolean;
	    archivedAt?: string;
	    settlementStatus?: string;
	    settlementSource?: string;
	    settlementValue?: number;
	    playerValue?: number;
	    superSubValue?: number;
	    settlementNote?: string;
	    settlementCheckedAt?: string;
	    fixtureStatus?: string;
	    analysisId?: string;
	    playerId?: number;
	    sampleEventIds?: number[];
	    historyNotes?: string[];
	    sampleRatings: number[];
	    fairModel?: FairModelDetails;
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
	    sampleSuperSubAvailable?: boolean[];
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
	        this.fixtureId = source["fixtureId"];
	        this.matchStart = this.convertValues(source["matchStart"], null);
	        this.matchName = source["matchName"];
	        this.addedAt = source["addedAt"];
	        this.includeSuperSub = source["includeSuperSub"];
	        this.archivedAt = source["archivedAt"];
	        this.settlementStatus = source["settlementStatus"];
	        this.settlementSource = source["settlementSource"];
	        this.settlementValue = source["settlementValue"];
	        this.playerValue = source["playerValue"];
	        this.superSubValue = source["superSubValue"];
	        this.settlementNote = source["settlementNote"];
	        this.settlementCheckedAt = source["settlementCheckedAt"];
	        this.fixtureStatus = source["fixtureStatus"];
	        this.analysisId = source["analysisId"];
	        this.playerId = source["playerId"];
	        this.sampleEventIds = source["sampleEventIds"];
	        this.historyNotes = source["historyNotes"];
	        this.sampleRatings = source["sampleRatings"];
	        this.fairModel = this.convertValues(source["fairModel"], FairModelDetails);
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
	        this.sampleSuperSubAvailable = source["sampleSuperSubAvailable"];
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
	
	export class SofaBrowserState {
	    status: string;
	    message: string;
	    address: string;
	    pending: boolean;
	    browser: string;
	
	    static createFrom(source: any = {}) {
	        return new SofaBrowserState(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.status = source["status"];
	        this.message = source["message"];
	        this.address = source["address"];
	        this.pending = source["pending"];
	        this.browser = source["browser"];
	    }
	}
	export class SuperbetCalendarEvent {
	    analysisEligibility?: AnalysisEligibility;
	    marketAvailability?: MatchMarketAvailability;
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
	        this.analysisEligibility = this.convertValues(source["analysisEligibility"], AnalysisEligibility);
	        this.marketAvailability = this.convertValues(source["marketAvailability"], MatchMarketAvailability);
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
	    warnings: string[];
	
	    static createFrom(source: any = {}) {
	        return new TeamDetectionResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.homeSlug = source["homeSlug"];
	        this.awaySlug = source["awaySlug"];
	        this.homeId = source["homeId"];
	        this.awayId = source["awayId"];
	        this.warnings = source["warnings"];
	    }
	}

}

