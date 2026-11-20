use super::*;
use futures_util::StreamExt;
use openmls::prelude::OpenMlsProvider as _;
use openmls::prelude::{
    BasicCredential, Ciphersuite, ExternalProposal, ExternalSender, GroupEpoch, GroupId,
    KeyPackageIn, ProtocolVersion, SenderExtensionIndex, VLBytes,
    tls_codec::{DeserializeBytes as _, Serialize as _},
};
use openmls_basic_credential::SignatureKeyPair;
use openmls_rust_crypto::OpenMlsRustCrypto;
use serde_json::Value;
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, accept_async, client_async};

#[tokio::test]
async fn real_mls_owner_audio_only_after_discord_executes_transition() -> Result<()> {
    timeout(std::time::Duration::from_secs(5), exchange_real_mls()).await?
}

async fn exchange_real_mls() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (wire, peer) = tokio::join!(
        async {
            let tcp = TcpStream::connect(address).await?;
            Ok::<_, anyhow::Error>(
                client_async(format!("ws://{address}"), MaybeTlsStream::Plain(tcp))
                    .await?
                    .0,
            )
        },
        async { Ok::<_, anyhow::Error>(accept_async(listener.accept().await?.0).await?) }
    );
    let mut peer = peer?;
    let route = Route {
        account: Id::parse("11").unwrap(),
        owner: Id::parse("22").unwrap(),
        guild: Id::parse("33").unwrap(),
        text: Id::parse("44").unwrap(),
    };
    let channel = Id::parse("55").unwrap();
    let mut wire = Wire::from_socket(wire?, route);
    let mut bot = Dave::new(route, channel, DAVE_PROTOCOL_VERSION, &mut wire).await?;
    let Some(Ok(Message::Binary(package))) = peer.next().await else {
        panic!("expected DAVE key package");
    };
    let [26, key_package @ ..] = package.as_ref() else {
        panic!("expected key package opcode");
    };

    let suite = Ciphersuite::MLS_128_DHKEMP256_AES128GCM_SHA256_P256;
    let backend = OpenMlsRustCrypto::default();
    let signer = SignatureKeyPair::new(suite.signature_algorithm()).unwrap();
    let external =
        ExternalSender::new(signer.public().into(), BasicCredential::new(vec![7]).into())
            .tls_serialize_detached()
            .unwrap();
    bot.signal(Frame::Binary(25, external.clone().into()), &mut wire)
        .await?;
    let mut owner = DaveSession::new(
        NonZeroU16::new(DAVE_PROTOCOL_VERSION).unwrap(),
        route.owner.get(),
        channel.get(),
        None,
    )?;
    owner.set_external_sender(&external)?;
    let owner_key_package = KeyPackageIn::tls_deserialize_exact_bytes(key_package)
        .unwrap()
        .validate(backend.crypto(), ProtocolVersion::Mls10)
        .unwrap();
    let add = ExternalProposal::new_add::<OpenMlsRustCrypto>(
        owner_key_package,
        GroupId::from_slice(&channel.get().to_be_bytes()),
        GroupEpoch::from(0),
        &signer,
        SenderExtensionIndex::new(0),
    )
    .unwrap()
    .tls_serialize_detached()
    .unwrap();
    let proposals = VLBytes::new(add).tls_serialize_detached().unwrap();
    let commit = owner
        .process_proposals(
            ProposalsOperationType::APPEND,
            &proposals,
            Some(&[route.account.get(), route.owner.get()]),
        )?
        .unwrap();
    assert!(owner.process_commit(&commit.commit)?);
    owner.prepare_transition(42, DAVE_PROTOCOL_VERSION)?;
    let mut welcome = 42u16.to_be_bytes().to_vec();
    welcome.extend(commit.welcome.unwrap());
    bot.signal(Frame::Binary(30, welcome.into()), &mut wire)
        .await?;
    let Some(Ok(Message::Text(ack))) = peer.next().await else {
        panic!("expected transition ready");
    };
    assert_eq!(
        serde_json::from_str::<Value>(ack.as_str())?,
        json!({"op":23,"d":{"transition_id":42}})
    );
    assert!(owner.execute_transition(42)?);
    let opus = [0x78, 0x55, 0x25, 0x52, 0x12, 0x85];
    let audio = owner.encrypt_opus(&opus)?;
    assert!(!bot.ready());
    assert!(bot.decrypt(&audio).is_none());
    assert!(bot.encrypt(&opus).is_err());

    bot.signal(Frame::Text(22, json!({"transition_id":42})), &mut wire)
        .await?;
    assert!(bot.ready());
    assert_eq!(bot.decrypt(&audio).as_deref(), Some(opus.as_slice()));
    assert!(
        bot.decrypt(&opus).is_none(),
        "raw Opus cannot pass as authenticated owner speech"
    );
    let reply = bot.encrypt(&opus)?;
    assert_eq!(
        owner.decrypt(route.account.get(), MediaType::AUDIO, &reply)?,
        opus
    );
    assert!(
        bot.signal(
            Frame::Text(21, json!({"transition_id":43,"protocol_version":0})),
            &mut wire
        )
        .await
        .is_err()
    );
    Ok(())
}
