import XCTest
@testable import Keystone

internal struct MockDelegate: KeystoneDelegate {
    
}

final class KeystoneTests: XCTestCase {
    func testConfigBuilding() async {
        let config = KeystoneConfig(userIdentifier: "XXX")
        let backend = MockBackend()
        var builder = await KeystoneAnalyzerBuilder(config: config, backend: backend, delegate: MockDelegate())
        
        builder.registerCategory(name: "ExampleEvent") { category in
            category.registerColumn(name: "MyColumn1", aggregators: [
                "X": { NumericStatsAggregator() },
                "Y": {
                    PredicateAggregator() {
                        guard case .text(let value) = $0 else {
                            return false
                        }
                        
                        return value == ":)"
                    }
                }
            ])
        }
        
        XCTAssertNoThrow {
            let state = try await builder.build()
            let eventCategories = await state.eventCategories
            
            XCTAssertEqual(eventCategories.count, 1)
        }
    }
}
