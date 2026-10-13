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
	export class TableSummary {
	    position: number;
	    matches: number;
	    points: number;
	
	    static createFrom(source: any = {}) {
	        return new TableSummary(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.position = source["position"];
	        this.matches = source["matches"];
	        this.points = source["points"];
	    }
	}
	export class FlowReactionSummary {
	    kind: string;
	    samples: number;
	    matches: number;
	    before: number;
	    after: number;
	    delta: number;
	
	    static createFrom(source: any = {}) {
	        return new FlowReactionSummary(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.kind = source["kind"];
	        this.samples = source["samples"];
	        this.matches = source["matches"];
	        this.before = source["before"];
	        this.after = source["after"];
	        this.delta = source["delta"];
	    }
	}
	export class FlowWindow {
	    points: number;
	    mean: number;
	    positive: number;
	    negative: number;
	
	    static createFrom(source: any = {}) {
	        return new FlowWindow(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.points = source["points"];
	        this.mean = source["mean"];
	        this.positive = source["positive"];
	        this.negative = source["negative"];
	    }
	}
	export class FlowStateSummary {
	    state: string;
	    matches: number;
	    window: FlowWindow;
	
	    static createFrom(source: any = {}) {
	        return new FlowStateSummary(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.state = source["state"];
	        this.matches = source["matches"];
	        this.window = this.convertValues(source["window"], FlowWindow);
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
	export class FlowMatchReference {
	    eventId: string;
	    revision: string;
	    // Go type: time
	    startedAt: any;
	    opponent: string;
	    venue: string;
	    homeName: string;
	    awayName: string;
	    homeScore?: number;
	    awayScore?: number;
	    available: boolean;
	    reason?: string;
	
	    static createFrom(source: any = {}) {
	        return new FlowMatchReference(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.eventId = source["eventId"];
	        this.revision = source["revision"];
	        this.startedAt = this.convertValues(source["startedAt"], null);
	        this.opponent = source["opponent"];
	        this.venue = source["venue"];
	        this.homeName = source["homeName"];
	        this.awayName = source["awayName"];
	        this.homeScore = source["homeScore"];
	        this.awayScore = source["awayScore"];
	        this.available = source["available"];
	        this.reason = source["reason"];
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
	export class TeamFlowOverview {
	    requested: number;
	    available: number;
	    matches: FlowMatchReference[];
	    states: FlowStateSummary[];
	    reactions: FlowReactionSummary[];
	    notes: string[];
	
	    static createFrom(source: any = {}) {
	        return new TeamFlowOverview(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.requested = source["requested"];
	        this.available = source["available"];
	        this.matches = this.convertValues(source["matches"], FlowMatchReference);
	        this.states = this.convertValues(source["states"], FlowStateSummary);
	        this.reactions = this.convertValues(source["reactions"], FlowReactionSummary);
	        this.notes = source["notes"];
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
	export class TeamOverview {
	    flow?: TeamFlowOverview;
	    name: string;
	    venue: string;
	    table?: TableSummary;
	    venueTable?: TableSummary;
	    form: string[];
	    venueForm: string[];
	    statsMatches: number;
	    metrics: Record<string, number>;
	
	    static createFrom(source: any = {}) {
	        return new TeamOverview(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.flow = this.convertValues(source["flow"], TeamFlowOverview);
	        this.name = source["name"];
	        this.venue = source["venue"];
	        this.table = this.convertValues(source["table"], TableSummary);
	        this.venueTable = this.convertValues(source["venueTable"], TableSummary);
	        this.form = source["form"];
	        this.venueForm = source["venueForm"];
	        this.statsMatches = source["statsMatches"];
	        this.metrics = source["metrics"];
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
	export class MatchOverview {
	    // Go type: time
	    asOf: any;
	    leagueId: number;
	    seasonId: number;
	    teams: Record<string, TeamOverview>;
	    notes: string[];
	
	    static createFrom(source: any = {}) {
	        return new MatchOverview(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.asOf = this.convertValues(source["asOf"], null);
	        this.leagueId = source["leagueId"];
	        this.seasonId = source["seasonId"];
	        this.teams = this.convertValues(source["teams"], TeamOverview, true);
	        this.notes = source["notes"];
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
	    playerId: number;
	    contextPending?: boolean;
	    sampleRatings: number[];
	    fairModel?: FairModelDetails;
	    matchOverview?: MatchOverview;
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
	        this.playerId = source["playerId"];
	        this.contextPending = source["contextPending"];
	        this.sampleRatings = source["sampleRatings"];
	        this.fairModel = this.convertValues(source["fairModel"], FairModelDetails);
	        this.matchOverview = this.convertValues(source["matchOverview"], MatchOverview);
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
	
	
	export class AutomaticSummary {
	    bookmaker: string;
	    eventId: string;
	    fixtureId: string;
	    match: string;
	    status: string;
	    stage: string;
	    value: number;
	    traps: number;
	    total: number;
	    // Go type: time
	    analyzedAt: any;
	    // Go type: time
	    expiresAt: any;
	    runId: string;
	    error?: string;
	
	    static createFrom(source: any = {}) {
	        return new AutomaticSummary(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.bookmaker = source["bookmaker"];
	        this.eventId = source["eventId"];
	        this.fixtureId = source["fixtureId"];
	        this.match = source["match"];
	        this.status = source["status"];
	        this.stage = source["stage"];
	        this.value = source["value"];
	        this.traps = source["traps"];
	        this.total = source["total"];
	        this.analyzedAt = this.convertValues(source["analyzedAt"], null);
	        this.expiresAt = this.convertValues(source["expiresAt"], null);
	        this.runId = source["runId"];
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
	
	export class FlowGraphPoint {
	    minute: number;
	    value: number;
	    state?: string;
	
	    static createFrom(source: any = {}) {
	        return new FlowGraphPoint(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.minute = source["minute"];
	        this.value = source["value"];
	        this.state = source["state"];
	    }
	}
	
	export class FlowReaction {
	    kind: string;
	    minute: number;
	    clock: string;
	    isHome?: boolean;
	    description: string;
	    homeScore?: number;
	    awayScore?: number;
	    stateBefore: string;
	    stateAfter: string;
	    before: FlowWindow;
	    after: FlowWindow;
	    eligible: boolean;
	
	    static createFrom(source: any = {}) {
	        return new FlowReaction(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.kind = source["kind"];
	        this.minute = source["minute"];
	        this.clock = source["clock"];
	        this.isHome = source["isHome"];
	        this.description = source["description"];
	        this.homeScore = source["homeScore"];
	        this.awayScore = source["awayScore"];
	        this.stateBefore = source["stateBefore"];
	        this.stateAfter = source["stateAfter"];
	        this.before = this.convertValues(source["before"], FlowWindow);
	        this.after = this.convertValues(source["after"], FlowWindow);
	        this.eligible = source["eligible"];
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
	
	
	
	export class HistoricalMatchFlow {
	    version: number;
	    eventId: string;
	    revision: string;
	    // Go type: time
	    fetchedAt: any;
	    homeId: number;
	    awayId: number;
	    homeName: string;
	    awayName: string;
	    homeScore?: number;
	    awayScore?: number;
	    timelineValid: boolean;
	    graphPoints: FlowGraphPoint[];
	    reactions: FlowReaction[];
	    notes: string[];
	
	    static createFrom(source: any = {}) {
	        return new HistoricalMatchFlow(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.version = source["version"];
	        this.eventId = source["eventId"];
	        this.revision = source["revision"];
	        this.fetchedAt = this.convertValues(source["fetchedAt"], null);
	        this.homeId = source["homeId"];
	        this.awayId = source["awayId"];
	        this.homeName = source["homeName"];
	        this.awayName = source["awayName"];
	        this.homeScore = source["homeScore"];
	        this.awayScore = source["awayScore"];
	        this.timelineValid = source["timelineValid"];
	        this.graphPoints = this.convertValues(source["graphPoints"], FlowGraphPoint);
	        this.reactions = this.convertValues(source["reactions"], FlowReaction);
	        this.notes = source["notes"];
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
	    sources: Record<string, MatchMarketAvailability>;
	
	    static createFrom(source: any = {}) {
	        return new MatchPreparation(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.qualified = source["qualified"];
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
	    sampleRatings: number[];
	    fairModel?: FairModelDetails;
	    matchOverview?: MatchOverview;
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
	        this.sampleRatings = source["sampleRatings"];
	        this.fairModel = this.convertValues(source["fairModel"], FairModelDetails);
	        this.matchOverview = this.convertValues(source["matchOverview"], MatchOverview);
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
	    marketAvailability?: MatchMarketAvailability;
	    analysisEligibility?: AnalysisEligibility;
	
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
	        this.marketAvailability = this.convertValues(source["marketAvailability"], MatchMarketAvailability);
	        this.analysisEligibility = this.convertValues(source["analysisEligibility"], AnalysisEligibility);
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

