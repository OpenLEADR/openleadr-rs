use openleadr_wire::target::TargetType;

/// Target for a query to the VTN
#[derive(Copy, Clone, Debug)]
pub enum Target<'a> {
    /// Target by a specific program name
    Program(&'a str),

    /// Target by a list of program names
    Programs(&'a [&'a str]),

    /// Target by a specific event name
    Event(&'a str),

    /// Target by a list of event names
    Events(&'a [&'a str]),

    /// Target by a specific VEN name
    VEN(&'a str),

    /// Target by a list of VEN names
    VENs(&'a [&'a str]),

    /// Target by a specific group name
    Group(&'a str),

    /// Target by a list of group names
    Groups(&'a [&'a str]),

    /// Target by a specific resource name
    Resource(&'a str),

    /// Target by a list of resource names
    Resources(&'a [&'a str]),

    /// Target by a specific service area
    ServiceArea(&'a str),

    /// Target by a list of service areas
    ServiceAreas(&'a [&'a str]),

    /// Target by a specific power service location
    PowerServiceLocation(&'a str),

    /// Target by a list of power service locations
    PowerServiceLocations(&'a [&'a str]),

    /// Target using some other kind of privately defined target type, using a single target value
    Other(&'a str, &'a str),

    /// Target using some other kind of privately defined target type, with a list of values
    Others(&'a str, &'a [&'a str]),
}

impl Target<'_> {
    /// Get the target label for this specific target
    pub fn target_label(&self) -> TargetType {
        match self {
            Target::Program(_) | Target::Programs(_) => TargetType::ProgramName,
            Target::Event(_) | Target::Events(_) => TargetType::EventName,
            Target::VEN(_) | Target::VENs(_) => TargetType::VENName,
            Target::Group(_) | Target::Groups(_) => TargetType::Group,
            Target::Resource(_) | Target::Resources(_) => TargetType::ResourceName,
            Target::ServiceArea(_) | Target::ServiceAreas(_) => TargetType::ServiceArea,
            Target::PowerServiceLocation(_) | Target::PowerServiceLocations(_) => {
                TargetType::PowerServiceLocation
            }
            Target::Other(p, _) | Target::Others(p, _) => TargetType::Private(p.to_string()),
        }
    }

    /// Get the list of target values for this specific target
    pub fn target_values(&self) -> &[&str] {
        match self {
            Target::Program(v) => std::slice::from_ref(v),
            Target::Programs(v) => v,
            Target::Event(v) => std::slice::from_ref(v),
            Target::Events(v) => v,
            Target::VEN(v) => std::slice::from_ref(v),
            Target::VENs(v) => v,
            Target::Group(v) => std::slice::from_ref(v),
            Target::Groups(v) => v,
            Target::Resource(v) => std::slice::from_ref(v),
            Target::Resources(v) => v,
            Target::ServiceArea(v) => std::slice::from_ref(v),
            Target::ServiceAreas(v) => v,
            Target::PowerServiceLocation(v) => std::slice::from_ref(v),
            Target::PowerServiceLocations(v) => v,
            Target::Other(_, v) => std::slice::from_ref(v),
            Target::Others(_, v) => v,
        }
    }
}
