export namespace main {
	
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
	    verdict: string;
	
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
	        this.verdict = source["verdict"];
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

