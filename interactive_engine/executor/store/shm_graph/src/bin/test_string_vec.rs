use shm_container::SharedStringVec;

fn main() {
    let string_vec: Vec<String> =
        vec!["AAA".to_string(), "BB".to_string(), "CCC".to_string(), "11111".to_string()];
    SharedStringVec::dump_vec("shared_string_vec.bin", &string_vec);

    let ssv = SharedStringVec::open("shared_string_vec.bin");
    let num = ssv.len();
    for i in 0..num {
        println!("vec[{}] = {}", i, ssv.get_unchecked(i));
    }
}
