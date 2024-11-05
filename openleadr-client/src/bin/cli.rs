use openleadr_client::ClientCredentials;
use openleadr_wire::program::ProgramContent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = openleadr_client::Client::with_url(
        "http://localhost:3000/".try_into()?,
        Some(ClientCredentials::new(
            "admin".to_string(),
            "admin".to_string(),
        )),
    );
    let _created_program = client.create_program(ProgramContent::new("name")).await?;
    // let created_program_1 = client.create_program(ProgramContent::new("name1")).await?;
    // let program = client.get_program_by_name("name").await?;
    // let created_event = program
    //     .create_event(program.new_event().with_event_name("prices3").with_priority(0))
    //     .await?;
    // let events = program.get_event_list(Filter::None).await?;
    // let reports = events[0].get_all_reports().await?;
    // let event = program.get_event(Target::Event("prices3")).await?;
    // dbg!(events);
    // dbg!(reports);

    // let programs: Vec<Program> = client.get_all_programs()?;
    // let programs = client.get_programs(TargetLabel::ProgramName, &["name"])?;

    // let program = client.get_program_by_id("id").await?;

    // let evt = program.send_event(Event {

    // })?;

    // let events = program.get_events(TargetLabel::EventName, &["name"], 0, 10)?;

    // program.get_event_by_name("prices").await?;

    // let events = program.get_all_events().await?;

    // // find the event you want, each event contains all relevant information to reconstruct periods
    // let event = events[0];

    // for interval in event.intervals {
    //     for payload in interval.payloads { // Iterator<Item = Payload
    //         // do something with the payload
    //         payload.timestamp;
    //         payload.unit;
    //         payload.currency;
    //         payload.value::<T = f64>();
    //     }
    // }

    // send a report
    // event.send_report(Report {

    // }).await?;

    // program.on_event(|evt| {

    // })?;

    Ok(())
}
